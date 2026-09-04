_eqbatch_completion() {
    local cur="${COMP_WORDS[COMP_CWORD]}"
    COMPREPLY=($(compgen -f -- "$cur"))
}

complete -o filenames -F _sqbatch_completion sqbatch