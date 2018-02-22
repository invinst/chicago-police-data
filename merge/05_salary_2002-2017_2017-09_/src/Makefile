# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/05_salary_2002-2017_2017-09_/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/salary_2002-2017_2017-09.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/salary_2002-2017_2017-09_profiles.csv.gz \
                input/salary_2002-2017_2017-09.csv.gz
	python $<


clean:
	rm output/*


# done
