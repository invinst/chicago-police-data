# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/02_unit-history__2016-12_p052262/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/unit-history__2016-12.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/unit-history__2016-12_profiles.csv.gz \
                input/unit-history__2016-12.csv.gz
	python $<


clean:
	rm output/*


# done
