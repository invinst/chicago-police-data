# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/salary_2002-2017_2017-09_/export/src/Makefile

.PHONY: all clean
# Additional files created: export.log, export.yaml,
# salary_2002-2017_2017-09_profiles.csv.gz

all: \
		output/salary_2002-2017_2017-09.csv.gz

output/salary_2002-2017_2017-09.csv.gz: \
		src/export.py \
		input/salary_2002-2017_2017-09.csv.gz \
		input/salary_2002-2017_2017-09_profiles.csv.gz
	python $<


clean:
	rm output/*


# done
