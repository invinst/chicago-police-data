# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/witness/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_witnesses.csv.gz

all: \
		output/witnesses.csv.gz

output/witnesses.csv.gz: \
		src/import.py \
                input/p046957_-_report_3_-_police_officer_witness_data_xi.xls
	python $<


clean:
	rm output/*


# done
