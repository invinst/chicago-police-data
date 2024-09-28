<% pair_tasks = lambda tasks: list(zip(tasks[-1::-1], tasks[-2::-1])) %>\
<%def name="escaped_newline()">\
<%text>\</%text>\
</%def>\
<%def name="list_dependencies(ls, indent=1)">\
	% for item in ls[:-1]:
	% if indent == 1:
	${item} ${escaped_newline()}
	% elif indent == 2:
		${item} ${escaped_newline()}
	% else:
${item} ${escaped_newline()}
	% endif
	% endfor
	% if indent == 1:
	${ls[-1]} 
	% elif indent == 2:
		${ls[-1]}
	% else:
${ls[-1]}
	% endif
</%def>\
%if tasks:
# ${foia_name}: ${', '.join([task.replace(f"{foia_name}_", "") for task in tasks])}

all: ${foia_name} ${escaped_newline()}
${list_dependencies([f"{foia_name}_{task}" for task in tasks], indent=1)}

.PHONY: ${foia_name} ${escaped_newline()}
${list_dependencies([f"{foia_name}_{task}" for task in tasks], indent=1)}
${foia_name}: ${escaped_newline()}
${list_dependencies([f"{foia_name}_{task}" for task in tasks])}
% for task_pair in pair_tasks(tasks):
${foia_name}_${task_pair[0]}: ${escaped_newline()}
	${foia_name}_${task_pair[1]}

${path}/${task_pair[0]}/input/${foia_name}.csv.gz: ${escaped_newline()}
		${path}/${task_pair[1]}/output/${foia_name}.csv.gz
	cd ${path}/${task_pair[0]}/input && ln -sf ../../${task_pair[1]}/output/${foia_name}.csv.gz

% endfor
% else:
${foia_name}_${task_name}: ${escaped_newline()}
		${output}

${output}: ${escaped_newline()}
		${script} ${escaped_newline()}
${list_dependencies(dependencies, indent=2)}\
	cd ${path}/${task_name} && python src/${task_name}.py
	
% endif