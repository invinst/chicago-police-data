# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/salary_2002-2017_2017-09_/assign-unique-ids/src/Makefile

.PHONY: all clean
# Additional files created: assign-unique-ids.log, assign-unique-ids.yaml
all: \
		output/salary_2002-2017_2017-09.csv.gz

output/salary_2002-2017_2017-09.csv.gz: \
		src/assign-unique-ids.py \
		input/salary_2002-2017_2017-09.csv.gz
	python3 $<


clean:
	rm output/*


# done
