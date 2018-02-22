# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/06_complaints-accused_2000-2016_2016-11_p046957/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/complaints-accused_2000-2016_2016-11.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/complaints-accused_2000-2016_2016-11_profiles.csv.gz \
                input/complaints-accused_2000-2016_2016-11.csv.gz
	python $<


clean:
	rm output/*


# done
