# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/WIP_complaints-accused_1967-1999_2016-12_/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# complaints-accused_1967-1999_2016-12.csv.gz,

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/complaints-accused_1967-1999_2016-12_profiles.csv.gz \
                input/complaints-accused_1967-1999_2016-12.csv.gz
	python $<


clean:
	rm output/*


# done
