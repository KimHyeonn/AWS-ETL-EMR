# AWS-ETL-EMR

aws-lambda 활용
(ETL.py / lambda_function.py)

s3 bucket 내 input/ 폴더에 새로운 파일 업로드시,
attribution / event 구분 후 해당하는 pyspark 스크립트 생성,
해당 스크립트를 단계로 포함하는 emr 클러스터 생성 및 실행, 자동 종료
결과물은 output/ 폴더에 업로드

athena 콘솔 내 table에 자동 적용, 업데이트 됨


''' lambda 관련, payload 나 event metadata 전달 형식, 방법 등을 숙지할 수 있었고, aws 서비스를 좀 더 이해할 수 있어짐'''
