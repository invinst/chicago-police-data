# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/fill_salary_ranks/src/Makefile

.PHONY: all clean
# Additional files created: fill_salary_ranks.log, fill_salary_ranks.yaml
# salary-ranks_2002-2017_2017-09.csv.gz

all: \
		output/salary-filled_2002-2017_2017-09.csv.gz

output/salary-filled_2002-2017_2017-09.csv.gz: \
		src/fill_salary_ranks.py \
		input/salary_2002-2017_2017-09.csv.gz \
		input/final-profiles.csv.gz
	python $<


clean:
	rm output/*


# done
