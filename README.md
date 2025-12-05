Repositorio GitHub:
https://github.com/MedievAlex/ADTi_Reto_MongoDB_-Alex_Ekaitz_Mikel-.git

He tenido que cambiar en el archivo classConfig.properties la siguiente linea:
Conn=mongodb://127.0.0.1:27017/?directConnection=true

Cambiandola a esta siguiente he conseguido que me funcione:
Conn=mongodb://localhost:27017/?directConnection=true