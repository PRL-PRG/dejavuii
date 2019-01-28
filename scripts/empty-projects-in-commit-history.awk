#!/usr/bin/env awk -f

function print_stuff () {
    if (last_line_is_project_id == 1) 
        print last_project",empty"
    else
        print last_project",nonempty"
}

BEGIN { 
    last_line_is_project_id = 0
    last_project = -1 
}

/^#/ {
    if (last_project != -1)
        print_stuff()

    last_line_is_project_id = 1
    last_project = $2

    next
}

{
    last_line_is_project_id = 0
}

END {
    print_stuff()
}
