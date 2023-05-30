#!/bin/bash

leerArchivo() {
    rutaCompleta=$1
    while IFS= read -r linea
    do
        echo "$linea"
    done < "$rutaCompleta"
}

rutaCompleta="$1"
leerArchivo "$rutaCompleta"
