# 공개 패키지 검토 기록

- Power BI 파일, 원본 데이터, `.env`, Kubernetes 인증 자료, SSH 키, 이미지 아카이브는 포함하지 않았습니다.
- 원본 검토 ZIP에서 비밀번호 고정 기본값이 발견되어 제거했으며, 관련 코드에서 `TRAFFIC_DB_PASSWORD` 환경변수가 필수입니다.
- Windows 전용 기본 데이터 경로는 프로젝트 상대 경로로 바꿨습니다.
- `docker-compose.yml`을 추가했습니다. 비밀번호는 환경변수로 참조합니다. 하지만 Spark Dockerfile, PostgreSQL JDBC 드라이버 JAR, 외부 Docker 볼륨, Kubernetes 인증 자료 및 SSH 키가 없어 **전체 서비스를 이 ZIP만으로 즉시 재현할 수 없습니다**.
- 제출된 파일만 기준으로 정적 구문 검사와 민감 문자열 검사를 수행했습니다. Docker/Airflow/Kubernetes 실제 실행 테스트를 새 패키지에 대해 수행한 것은 아닙니다.
- Kubernetes API 호스트/인증 경로 및 Spark 이미지명은 특정 개발 환경에 종속되어 있습니다. 공개 저장소에 올리더라도 별도의 배포·재현 가이드가 필요합니다.
- `.env.example`의 `SET_YOUR_VALUE`는 설명용 자리표시자입니다.
- 기존 개발 환경에서 사용한 비밀번호는 향후 변경하는 편이 안전합니다.
