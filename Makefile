test:
	pylint tap_circle_ci -d broad-except,chained-comparison,empty-docstring,\
	fixme,invalid-name,line-too-long,missing-class-docstring,\
	missing-function-docstring,missing-module-docstring,no-else-raise,\
	no-else-return,too-few-public-methods,too-many-arguments,\
	too-many-branches,too-many-lines,too-many-locals,ungrouped-imports,\
	wrong-spelling-in-comment,wrong-spelling-in-docstring

test_alpha:
	pylint tap_circle_ci -d broad-except,chained-comparison,empty-docstring,\
	fixme,invalid-name,line-too-long,missing-class-docstring,\
	missing-function-docstring,missing-module-docstring,no-else-raise,\
	no-else-return,too-few-public-methods,too-many-arguments,\
	too-many-branches,too-many-lines,too-many-locals,ungrouped-imports,\
	wrong-spelling-in-comment,wrong-spelling-in-docstring,\
	unnecessary-comprehension,too-many-nested-blocks,redefined-builtin,\
	bad-whitespace,dangerous-default-value

