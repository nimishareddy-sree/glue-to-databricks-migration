.PHONY: setup data run test lint clean

setup:
	pip install -r requirements.txt

data:
	python data/generate_sample_data.py --rows 5000 --out data/raw

run: data
	python jobs/run_pipeline.py --env local --source-path data/raw/batch_1
	python jobs/run_pipeline.py --env local --source-path data/raw/batch_2

test:
	pytest -q

lint:
	ruff check src tests jobs data

clean:
	rm -rf _lakehouse spark-warehouse metastore_db data/raw .pytest_cache
