# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# merge/11_roster__2018-03_p441436/src/Makefile

.PHONY: all clean
# Additional files created: merge.log, merge.yaml,
# input/roster__2018-03.csv.gz

all: \
		output/officer-reference.csv.gz

output/officer-reference.csv.gz: \
		src/merge.py \
		input/officer-reference.csv.gz \
                input/roster__2018-03_profiles.csv.gz \
                input/roster__2018-03.csv.gz
	python $<


clean:
	rm output/*


# done
