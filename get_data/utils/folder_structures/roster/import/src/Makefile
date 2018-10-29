# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/roster__2018-03_p441436/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_roster__2018-03.csv.gz

all: \
		output/roster__2018-03.csv.gz

output/roster__2018-03.csv.gz: \
		src/import.py \
		input/P441436-current_and_former_CPD_employee_list_run_15_Mar_2018_by_CPD_IT-redacted_1.xlsx
	python3 $<


clean:
	rm output/*


# done
