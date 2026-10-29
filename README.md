# Kubernetes 기반 교통 데이터 배치 파이프라인

> Python · PostgreSQL · Apache Airflow · Apache Spark · Docker · Kubernetes · Power BI

## 프로젝트 소개

교통 측정 데이터를 PostgreSQL에 적재하고, Apache Airflow에서 Kubernetes 기반 Spark 작업을 실행해 날짜·수집 유형·차량 종류별로 집계하는 배치 데이터 엔지니어링 포트폴리오입니다. SQL 정합성 검증, 작업 실행 이력 관리, 성공한 작업의 중복 제출 방지까지 구현했습니다.

## 주요 검증 결과

| 항목 | 실제 확인 결과 |
|---|---|
| 원천 데이터 | 20,125건 |
| 집계 결과 | 79개 그룹 |
| 집계 차원 | `datum`, `source_type`, `kfz_klasse` |
| 데이터 정합성 | 원천 그룹별 집계와 결과 테이블 일치 |
| Airflow DAG | `traffic_kubernetes_pipeline` 수동 실행 2회 성공 (서로 다른 코드 버전) |
| Kubernetes Spark Driver | `Succeeded` 확인 |
| 실행 이력 | `spark_job_execution_log`에 `SUCCEEDED` 기록 확인 |
| 중복 제출 방지 | 동일 Run ID의 재테스트에서 `ALREADY_SUCCEEDED` 반환, Driver 1개 유지 |

*위 결과는 로컬 개발 환경에서 검증한 수치입니다. 처리 건수만으로 대규모 데이터 처리 성능을 입증하지는 않습니다.*

## 시스템 아키텍처

```text
교통 측정 CSV
    │
    ▼
Python 적재 / 증분 ETL
    │
    ▼
PostgreSQL: traffic_raw
    │
    ▼
Apache Airflow ──────┐
    │                 │
    ▼                 ▼
Kubernetes API    PostgreSQL: spark_job_execution_log
    │                 (Spark 실행 상태)
    ▼
Spark Driver + Executor
    │
    ▼
그룹별 집계 및 UPSERT
    │
    ▼
PostgreSQL: traffic_daily_summary
    │
    ├── SQL 정합성 검증
    │
    └── Power BI 분석 리포트
```

## 기술 스택

| 영역 | 기술 및 역할 |
|---|---|
| 수집 및 적재 | Python, 증분 ETL |
| 저장 및 검증 | PostgreSQL, SQL |
| 작업 오케스트레이션 | Apache Airflow |
| 데이터 집계 | Apache Spark |
| 컨테이너 실행 | Docker |
| Spark 실행 환경 | Kubernetes |
| 데이터 시각화 | Power BI Desktop |

## 주요 구현 내용

### 1. 수집·적재 및 집계

- Python 스크립트로 교통 측정 데이터를 PostgreSQL에 적재합니다.
- Spark에서 측정 날짜(`datum`), 수집 유형(`source_type`), 차량 종류(`kfz_klasse`)를 기준으로 차량 수(`vehicle_count`)를 집계합니다.
- UPSERT 방식으로 집계 결과를 갱신해 동일 그룹의 중복 행 생성을 방지하도록 구성했습니다.

관련 코드: `scripts/load_csv.py`, `scripts/incremental_load.py`, `spark/write_summary.py`, `sql/merge_traffic_summary.sql`

### 2. Airflow → Kubernetes → Spark 실행

- Airflow DAG에서 Kubernetes API로 Spark 실행을 제출하고 Driver Pod의 완료 여부를 확인합니다.
- 실행 결과는 PostgreSQL에 기록하며, Spark 처리 후 SQL 검증 작업을 수행합니다.

관련 코드: `dags/traffic_kubernetes_pipeline.py`, `dags/traffic_k8s_client.py`

### 3. 데이터 정합성 검증

- `traffic_raw`의 그룹별 합계와 `traffic_daily_summary`를 비교합니다.
- 집계 그룹 누락 및 차량 수 불일치를 검출하도록 구성했습니다.
- 검증한 실행에서는 총 20,125건이 79개 그룹으로 집계되었고 불일치가 확인되지 않았습니다.

### 4. 실행 이력 및 중복 제출 방지

- PostgreSQL 테이블 `spark_job_execution_log`에 현재 상태를 저장합니다.
- 설계한 상태값은 `PENDING`, `SUBMITTED`, `RUNNING`, `SUCCEEDED`, `FAILED`입니다.
- 동일 Airflow Run ID에 `SUCCEEDED` 이력이 존재하면 Spark 작업을 재제출하지 않고 `ALREADY_SUCCEEDED`를 반환합니다.
- 실제 재테스트에서 기존 성공 이력을 이용한 제출 생략을 확인했습니다.

**구현 한계:** 상태 전환 이력 전체를 보관하지 않으며, 자동 재시도는 비활성화되어 있습니다. 모든 장애·동시 실행·Pod 유실 시나리오에 대한 복구 안정성을 검증한 것은 아닙니다.

### 5. Power BI 분석 리포트

- 전체 교통량 및 수집 유형별 KPI
- 측정 날짜별 교통량 추이
- 차량 종류별 분포
- 날짜 슬라이서를 활용한 상호작용형 필터링

**분석상 주의:** `TRACKING`과 `KONTROLLE`는 데이터 수집 범위가 동일한지 확인되지 않았으므로 단순 총량 차이를 AI 정확도나 측정 오차율로 해석하지 않습니다.

## 주요 파일

```text
dags/
  traffic_pipeline.py
  traffic_kubernetes_pipeline.py
  traffic_k8s_client.py
scripts/
  load_csv.py
  incremental_load.py
spark/
  traffic_aggregation.py
  write_summary.py
  benchmark_synthetic.py
sql/
  merge_traffic_summary.sql
README.md
docker-compose.yml
.env.example
```

테스트 및 보조 스크립트는 해당 폴더에 별도로 포함되어 있습니다. `docker-compose.yml`은 비밀번호 값을 환경변수로 참조하는 공개용 설정으로 포함했습니다. 다만 **Spark Dockerfile, JDBC 드라이버 JAR, SSH 키, Kubernetes 인증 파일 및 배포 매니페스트가 포함되지 않아 이 ZIP만으로 전체 환경을 즉시 실행할 수는 없습니다.** 해당 의존성은 개인 개발 환경별로 별도 구성해야 합니다.

## 보안 및 데이터 공개 원칙

`.env`, `k8s-auth/`, `ssh/`, 로컬 가상환경, Docker 이미지 아카이브 및 실제 원본 데이터는 기본적으로 GitHub 공개 대상에서 제외합니다. 공개 전 Docker Compose 및 전체 추적 파일에 인증 정보나 비공개 데이터가 없는지 추가 검토가 필요합니다. Power BI `.pbix`는 포함된 데이터의 공개 가능 여부를 검토한 뒤 공개 여부를 결정합니다.

## 개선 계획

- 실행 시간·성공률·처리량을 시각화하는 Pipeline Health Monitor
- 원천 데이터·집계 데이터의 품질 지표를 결합한 Data Trust Score
- 추가 데이터 확보 후 이상 탐지·분포 변화 분석
- 장애 복구 및 동시 실행 테스트 확대
- 재현 가능한 실행 가이드 및 아키텍처 다이어그램 보완

---

**프로젝트 목적:** 단순한 시각화가 아니라 수집 → 저장 → 분산 처리 → 오케스트레이션 → 품질 검증 → 운영 상태 확인으로 이어지는 데이터 엔지니어링 업무 흐름을 직접 구현하고 검증하는 것입니다.


## 공개용 코드 실행 시 주의사항

보안을 위해 예제 코드에서 데이터베이스 비밀번호의 고정 기본값을 제거했습니다. 실행하려면 `TRAFFIC_DB_PASSWORD` 환경변수를 별도로 설정해야 합니다. `.env.example`의 자리는 실제 환경에 맞게 채우되 `.env`는 커밋하지 마세요. 원래 로컬 환경에서 실행한 버전과 이 공개용 소스의 설정은 일부 다릅니다.
