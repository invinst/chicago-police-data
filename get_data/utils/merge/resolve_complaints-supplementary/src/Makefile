# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/resolve_complaints-supplementary/src/Makefile

.PHONY: all clean
# Additional files created: resolve_complaints-supplementary.log, resolve_complaints-supplementary.yaml,
# output/complaints-complainants.csv.gz
# output/complaints-investigators.csv.gz
# output/complaints-CPD-witnesses.csv.gz
all: \
		output/complaints-victims.csv.gz

output/complaints-victims.csv.gz: \
		src/resolve_complaints-supplementary.py \
		input/complaints-CPD-witnesses_2000-2018_2018-03.csv.gz \
		input/complaints-complainants_2000-2016_2016-11.csv.gz \
		input/complaints-complainants_2000-2018_2018-03.csv.gz \
		input/complaints-investigators_2000-2016_2016-11.csv.gz \
		input/complaints-investigators_2000-2018_2018-03.csv.gz \
		input/complaints-victims_2000-2016_2016-11.csv.gz \
		input/complaints-victims_2000-2018_2018-03.csv.gz \
		input/complaints-witnesses_2000-2016_2016-11.csv.gz
	python $<


clean:
	rm output/*


# done
