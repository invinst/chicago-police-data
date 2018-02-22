# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/04_awards_1967-2017_2017-08_p061715/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/awards_1967-2017_2017-08.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/awards_1967-2017_2017-08_profiles.csv.gz \
                input/awards_1967-2017_2017-08.csv.gz
	python $<


clean:
	rm output/*


# done
