# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/generate_TRR_flags/src/Makefile

.PHONY: all clean
# Additional files created: generate_TRR_flags.log, generate_TRR_flags.yaml

all: \
		output/TRR-main_2004-2016_2016-09.csv.gz

output/TRR-main_2004-2016_2016-09.csv.gz: \
		src/generate_TRR_flags.py \
		input/TRR-main_2004-2016_2016-09.csv.gz \
		input/TRR-actions-responses_2004-2016_2016-09.csv.gz \
		input/TRR-weapon-discharges_2004-2016_2016-09.csv.gz
	python $<


clean:
	rm output/*


# done
