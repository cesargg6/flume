#!/bin/bash

leerArchivo() {
    rutaCompleta=$1
    while IFS= read -r linea
    do
        # Ejecutar el script Pig
        pig -x mapreduce -f /content/pruebas.pig -param input_path='/content/HotelBookings.csv'
        echo "$linea"
    done < "$rutaCompleta"
}

rutaCompleta="$1"
leerArchivo "$rutaCompleta"
