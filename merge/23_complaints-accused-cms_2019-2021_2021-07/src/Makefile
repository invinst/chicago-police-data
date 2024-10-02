# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)


.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/complaints-accused_2000-2018_2018-07.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/complaints-accused_2000-2018_2018-07_profiles.csv.gz \
                input/complaints-accused_2000-2018_2018-07.csv.gz
	python $<


clean:
	rm output/*


# done
