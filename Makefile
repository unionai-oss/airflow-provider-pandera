test:
	@airflow db reset -y && pytest
