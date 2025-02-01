This repo has Airflow DAG that prepare data for [streamlit app](https://github.com/Eliakats/St_coeff_app). Raw data is got by [python script](https://github.com/skizel1/seller_plan-wh_coef).

Run:
1. Add env variables in docker-compose.yaml:
 - S3_BUCKET_NAME
 - AWS_ACCESS_KEY_ID
 - AWS_SECRET_ACCESS_KEY

2. Run `docker-compose up -d --build` in the repo. Your environment must have at least 4GB of RAM to work properly (even more in order to aggregate parquets in memory)

3. Use `localhost:8080` to reach Airflow UI