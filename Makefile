test:
	@airflow db reset -y && pytest --cov-config=.coveragerc --cov
