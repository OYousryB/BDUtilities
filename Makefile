build:
	sbt clean package assembly

tpchGen:
	./scripts/tpch/generate-all.sh

tpcdsGen:
	./scripts/tpcds/generate-all.sh
