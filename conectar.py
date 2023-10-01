# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 17:51:02 2023

@author: maxan
"""

import pyodbc as dbc
import pandas as pd

##se crea el trigger para actualizar los nombres de los paises en FIFA_WCS
##tras insertar datos
def trig_sum(connect):
    cur_tem= connect.cursor()
    com= """ 
    CREATE or ALTER TRIGGER tr_summ
    on FIFA_WCS
    for insert
    as
    set nocount on
    update FIFA_WCS SET HOST= (CASE WHEN (HOST = 'Bulgaria**') THEN 'Bulgaria'
    WHEN (HOST = 'Israel*') THEN 'Israel'
    WHEN (HOST = 'Serbia') THEN 'Serbia and Montenegro'
    WHEN (HOST = 'United States') THEN 'USA'
    WHEN (HOST = 'Soviet Union') THEN 'Russia'
    ELSE (HOST)
    END)
    update FIFA_WCS
    SET CHAMPION = (CASE WHEN (CHAMPION = 'Bulgaria**') THEN 'Bulgaria'
    WHEN (CHAMPION = 'Israel*') THEN 'Israel'
    WHEN (CHAMPION = 'Serbia') THEN 'Serbia and Montenegro'
    WHEN (CHAMPION = 'United States') THEN 'USA'
    WHEN (CHAMPION = 'Soviet Union') THEN 'Russia'
    ELSE (CHAMPION)
    END) 
    update FIFA_WCS
    SET RUNNER_UP = (CASE WHEN (RUNNER_UP = 'Bulgaria**') THEN 'Bulgaria'
    WHEN (RUNNER_UP = 'Israel*') THEN 'Israel'
    WHEN (RUNNER_UP = 'Serbia') THEN 'Serbia and Montenegro'
    WHEN (RUNNER_UP = 'United States') THEN 'USA'
    WHEN (RUNNER_UP = 'Soviet Union') THEN 'Russia'
    ELSE (RUNNER_UP)
    END) 
    update FIFA_WCS
    SET THIRD_PLACE = (CASE WHEN (THIRD_PLACE = 'Bulgaria**') THEN 'Bulgaria'
    WHEN (THIRD_PLACE = 'Israel*') THEN 'Israel'
    WHEN (THIRD_PLACE = 'Serbia') THEN 'Serbia and Montenegro'
    WHEN (THIRD_PLACE = 'United States') THEN 'USA'
    WHEN (THIRD_PLACE = 'Soviet Union') THEN 'Russia'
    ELSE (THIRD_PLACE)
    END)
    """
    cur_tem.execute(com)
    cur_tem.commit()
    cur_tem.close()
    
##se crea el trigger para actualizar los nombres de los paises en FIFA_YEARS_INFO
##tras insertar datos
def trig_years(connect):
    cur_tem= connect.cursor()
    com= """ 
    CREATE or ALTER TRIGGER tr_years
    on FIFA_YEARS_INFO
    for insert
    as
    set nocount on
    update FIFA_YEARS_INFO SET TEAM= (CASE WHEN (TEAM= 'Bulgaria**') THEN 'Bulgaria'
    WHEN (TEAM = 'Israel*') THEN 'Israel'
    WHEN (TEAM = 'Serbia') THEN 'Serbia and Montenegro'
    WHEN (TEAM = 'United States') THEN 'USA'
    WHEN (TEAM = 'Soviet Union') THEN 'Russia'
    ELSE (TEAM)
    END)
    """
    cur_tem.execute(com)
    cur_tem.commit()
    cur_tem.close()

#operacion para mostrar los paises que han sido campeones y su año resp.
def op_1(conect, nom):
    cur_select= conect.cursor()
    com= 'SELECT YEAR, CHAMPION FROM '+nom
    cur_select.execute(com)
    result=cur_select.fetchall()
    for elemento in result:
        print("En el año "+elemento[0]+" el campeón fue "+elemento[1]+".")
    cur_select.close()

#operacion para mostrar los 5 paises con mayor cantidad de goles que han hecho    
def op_2(conect, nom):
    cur_tem= conect.cursor()
    com= 'SELECT TEAM, SUM(GOALS_FOR) as TOTAL FROM '+nom+' GROUP BY TEAM ORDER BY 2 DESC'
    cur_tem.execute(com)
    i=0
    print('Los 5 paises con mayor cantidad de goles que han hecho en todos los mundiales son:')
    while i<5:
        fila= cur_tem.fetchone()
        print(fila[0], fila[1])
        i=i+1
    cur_tem.close()

#operacion para mostrar top 5 de los paises con mayor cantidad de veces
#que han obtendio el tercer puesto    
def op_3(conect, nom):
    cur_tem= conect.cursor()
    com='SELECT THIRD_PLACE, COUNT(*) AS COUNT FROM '+nom+' GROUP BY THIRD_PLACE ORDER BY 2 DESC'
    cur_tem.execute(com)
    i=0
    print('TOP 5 países con mayor cantidad de veces han obtenido el tercer puesto')
    while i<5:
        fila= cur_tem.fetchone()
        print(fila[0], fila[1])
        i=i+1
    cur_tem.close()

#operacion para obtener el equipo con mas goles recibidos en contra    
def op_4(conect, nom):
    cur_tem= conect.cursor()
    com= 'SELECT TEAM, SUM(GOALS_AGAINST) as TOTAL FROM '+nom+' GROUP BY TEAM ORDER BY 2 DESC'
    cur_tem.execute(com)
    print('El pais que mas goles ha recibido en contra es:')
    result= cur_tem.fetchone()
    print(result[0], result[1])
    cur_tem.close()

#operacion para buscar un pais y entregar toda su informacion disponible
def op_5(conect, nom):
    cur_tem= conect.cursor()
    pais=input("Indique el pais a buscar: ")
    com= 'SELECT * FROM '+nom+' WHERE TEAM = ?'
    cur_tem.execute(com, pais)
    result= cur_tem.fetchall()
    if len(result)>0:
        for elemento in result:
            print( elemento[1], " quedo en la posición ", elemento[0], "en el año", elemento[10],"con ",elemento[9] ,"puntos. En este año, participo en ", elemento[2], "partidos, de los cuales gano ", elemento[3], ", empato ", elemento[4], " y perdio ", elemento[5], ". Además, tuvo un total ",elemento[6], " goles a favor y ", elemento[7], " goles en contra, obteninendo una diferencia de ",elemento[8]," goles.\n ")
    else:
        print("No hay informacion relacionada al pais ",pais)
    cur_tem.close()
    
#operacion para obtener top 3 de paises que mas han jugado
def op_6(conect, nom):
    cur_tem= conect.cursor()
    com_select= 'SELECT TOP 3 TEAM, SUM(GAMES_PLAYED) AS TOTAL_PLAYED FROM '+nom+' GROUP BY TEAM ORDER BY 2 DESC'
    cur_tem.execute(com_select)
    result= cur_tem.fetchall()
    print('TOP 3 paises en el mundial')
    dic= {}
    for elemento in result:
        if elemento[0] not in dic:
            dic[elemento[0]]= []
        com_tem= 'SELECT YEAR FROM '+nom+' WHERE TEAM = ?'
        cur_tem.execute(com_tem, elemento[0])
        tem= cur_tem.fetchall()
        for i in tem:
            dic[elemento[0]].append(i[0])
    count=1        
    for llave in dic:
        print("Pais número ",count," : ",llave,"\n Años participantes : ", dic[llave])
        count+=1
    dic.clear()
    cur_tem.close()
    
# Se debe mostrar el pais que historicamente tiene la mayor tasa de partidos
# ganados en relacion a su total de partidos jugados
# Se debe mostrar el nombre del pais y la tasa calculada.
def op_7(conect, nom):
    cur_tem = conect.cursor()
    com= 'SELECT TEAM, SUM(WIN) AS TOTAL_WIN, SUM(GAMES_PLAYED) AS TOTAL_PLAYED FROM '+nom+' GROUP BY TEAM'
    cur_tem.execute(com)
    result = cur_tem.fetchall()
    lista = []
    for content in result:
        lista.append([float(content[1]/content[2]) ,content[0] ])
    lista.sort(reverse=True)
    print('el pais con mas \"winrate\" es : '+lista[0][1]+', con una tasa de victoria del ',lista[0][0])
    lista.clear() # reiniciar la lista para la recusividad
    cur_tem.close()
 
#Mostrar los paises que han ganado el mundial cuando a la vez fueron el pais anfitrion. Se debe mostrar el pais y el año.
def op_8(conect, nom):
    cur_tem = conect.cursor()
    com = 'SELECT YEAR, HOST, CHAMPION FROM FIFA_WCS'
    cur_tem.execute(com)
    result = cur_tem.fetchall()
    for content in result:
        if content[1] == content[2]:
            print('el campeon del año '+content[0]+' es el mismo huesped del mundial: '+content[2] +' (el huesped es: '+content[1]+' )')
    cur_tem.close()
    
# Se debe mostrar el pais que mas veces ha estado entre los ganadores del primer,segundo o tercer lugar.
def op_9(conect, nom):
    cur_tem = conect.cursor()
    com = 'SELECT CHAMPION, RUNNER_UP, THIRD_PLACE FROM FIFA_WCS' 
    cur_tem.execute(com)
    result = cur_tem.fetchall()
    lista = []
    datos = {}
    for i in result:
        lista.extend([i[0], i[1], i[2]])
    lista.sort()
    for pais in lista:
        if pais not in datos:
            datos[pais] = 0
        datos[pais] += 1   
    best = ''
    top = 0
    for fin in datos:
        if datos[fin] > top:
            best = fin
            top = datos[fin]
    print ('el pais que más veces ha tocado el podio es: '+best+', y lo ha hecho '+str(top)+' veces.')
    lista.clear()
    datos.clear()
    cur_tem.close()
    
    
#operacion para obtener los 2 paises con mayor cantidad de veces que se han
#peleado por el primer y segundo lugar entre si
def op_10(conect):
    cur_tem= conect.cursor()
    nom_table= 'rivals'
    cur_tem.execute('DROP TABLE IF EXISTS '+nom_table)
    cur_tem.execute('CREATE TABLE rivals (YEAR integer, RIVALES VARCHAR(75))')
    com_insert= 'INSERT INTO '+nom_table+' VALUES (?,?)'
    #se crea view para ver el primer y segundo lugar en cada año
    nom_view='rivales'
    cur_tem.execute('DROP VIEW IF EXISTS '+nom_view)
    com_view= """
    CREATE VIEW rivales 
    AS SELECT YEAR, CHAMPION, RUNNER_UP 
    FROM FIFA_WCS
    """
    cur_tem.execute(com_view)
    cur_tem.execute('SELECT * FROM '+nom_view)
    result= cur_tem.fetchall()
    for elemento in result:
        temp= [elemento[1], elemento[2]]
        temp.sort()
        rivales= temp[0]+', '+temp[1]
        cur_tem.execute(com_insert, elemento[0], rivales)
        cur_tem.commit()
    com_order= 'SELECT RIVALES, COUNT(*) AS COUNT FROM rivals GROUP BY RIVALES ORDER BY 2 DESC'
    cur_tem.execute(com_order)
    pareja=cur_tem.fetchone()
    print('Los 2 paises con mayor cantidad de veces que se han peleado por el primer y segundo lugar entre si son:', pareja[0])
    cur_tem.close()


##datos para la conexion: nombre del sevidor y de la base de datos a conectar
server= 'DESKTOP-45BJ9G6\SQLEXPRESS'        ##nombre del servidor
nombre_bd= 'FIFA'                           ##nombre de la base de datos

##se crea la conexion de python con SQL
try:
    conexion= dbc.connect('DRIVER={SQL Server}; SERVER='+server+';DATABASE='
                          +nombre_bd+';Trusted_Connection=yes')
    print("Conexion exitosa.\n")
except Exception as ex:
    print("Error al conectarse a la base de datos.\n")
    print(ex)
    exit()

##creacion de cursor para realizar operaciones en SQL
cursor= conexion.cursor()

##lectura del archivo FIFA - World Cup Summary.csv
FIFA_WCS =pd.read_csv('FIFA - World Cup Summary.csv')
valores_WCS= FIFA_WCS.values.tolist()

##nombres de tablas para almacenar la informacion del archivos csv del summary 
##con los nombres actualizados y un respaldo de la informacion original
nom_summ= 'FIFA_WCS'
nom_res_summ= 'res_FIFA_WCS'
if cursor.tables(table=nom_res_summ).fetchone():
    cursor.execute('DROP TABLE '+nom_res_summ)
    print('Se elimino la tabla'+nom_res_summ+'\n')
    conexion.commit()
    
cursor.execute('CREATE TABLE res_FIFA_WCS (YEAR varchar(4), HOST varchar(50), CHAMPION varchar(50), RUNNER_UP varchar(50), THIRD_PLACE varchar(50),TEAMS integer, MATCHED_PLAYED integer, GOALS_SCORED integer, AVG_GOALS_PER_GAME float, PRIMARY KEY (YEAR))')
print('Se creo la tabla res_FIFA_WCS\n')

if cursor.tables(table=nom_summ).fetchone():
    cursor.execute('DROP TABLE '+nom_summ)
    print('Se elimino la tabla '+nom_summ+'\n')
    conexion.commit()
    
cursor.execute('CREATE TABLE FIFA_WCS (YEAR varchar(4), HOST varchar(50), CHAMPION varchar(50), RUNNER_UP varchar(50), THIRD_PLACE varchar(50),TEAMS integer, MATCHED_PLAYED integer, GOALS_SCORED integer, AVG_GOALS_PER_GAME float, PRIMARY KEY (YEAR))')
print('Se creo la tabla FIFA_WCS\n')

for fila in valores_WCS:
    com1= 'INSERT INTO res_FIFA_WCS VALUES (?,?,?,?,?,?,?,?,?)'
    cursor.execute(com1,fila)
    conexion.commit()
    com2= 'INSERT INTO FIFA_WCS VALUES (?,?,?,?,?,?,?,?,?)'
    cursor.execute(com2,fila)
    conexion.commit()
    trig_sum(conexion)

print('Se insertaron los datos a la tabla res_FIFA_WCS')
print('Se insertaron los datos a la tabla FIFA_WCS')

##nombres de tablas para almacenar la informacion de los archivos csv de cada 
##año con los nombres actualizados y un respaldo de la informacion original
nom_years= 'FIFA_YEARS_INFO'
res_nom_years= 'res_FIFA_YEARS_INFO'

if cursor.tables(table=nom_years).fetchone():
    cursor.execute('DROP TABLE '+nom_years)
    print('Se elimino la tabla '+nom_years+'\n')
    conexion.commit()
    
if cursor.tables(table=res_nom_years).fetchone():
    cursor.execute('DROP TABLE '+res_nom_years)
    print('Se elimino la tabla '+res_nom_years+'\n')
    conexion.commit()
    
comm_year_res= 'CREATE TABLE '+res_nom_years+'(POSITION integer, TEAM varchar(50), GAMES_PLAYED integer, WIN integer, DRAW integer, LOSS integer, GOALS_FOR integer, GOALS_AGAINST integer, GOAL_DIFFERENCE varchar(5), POINTS integer, YEAR varchar(5))'
cursor.execute(comm_year_res)
print('Se creo la tabla res_FIFA_YEARS_INFO\n')

comm_year= 'CREATE TABLE '+nom_years+'(POSITION integer, TEAM varchar(50), GAMES_PLAYED integer, WIN integer, DRAW integer, LOSS integer, GOALS_FOR integer, GOALS_AGAINST integer, GOAL_DIFFERENCE varchar(5), POINTS integer, YEAR varchar(5))'
cursor.execute(comm_year)
print('Se creo la tabla FIFA_YEARS_INFO\n')

##años de la tabla FIFA_WCS para creacion del resto de tablas
cursor.execute('SELECT YEAR FROM FIFA_WCS')
years= cursor.fetchall()
for year in years:
    i=year[0]
    temp= 'FIFA - '+i+'.csv'
    FIFA_i =pd.read_csv(temp)
    FIFA_i['AÑO']= i
    valores_i= FIFA_i.values.tolist()
    comm_insert_years= 'INSERT INTO '+nom_years+' VALUES(?,?,?,?,?,?,?,?,?,?,?)'
    comm_insert_years_res= 'INSERT INTO '+res_nom_years+' VALUES(?,?,?,?,?,?,?,?,?,?,?)'
    for fila in valores_i:
        cursor.execute(comm_insert_years_res,fila)
        conexion.commit()
        cursor.execute(comm_insert_years,fila)
        trig_years(conexion)
    print('Se añadieron los datos relacionados al archivo '+temp)
##act_datos(conexion, nom_years, 'TEAM')


##se solicita el numero de operacion a realizar
print("Menú de operaciones:")
seguir= True
while seguir:
    operacion= input("Indique el numero de operación a realizar: ")
    if operacion == "1":
        op_1(conexion,nom_summ)
    elif operacion == "2":
        op_2(conexion,nom_years)
    elif operacion == "3":
        op_3(conexion,nom_summ)
    elif operacion == "4":
        op_4(conexion,nom_years)
    elif operacion == "5":
        op_5(conexion,nom_years)
    elif operacion == "6":
        op_6(conexion,nom_years)
    elif operacion == "7":
        op_7(conexion,nom_years)
    elif operacion == "8":
        op_8(conexion,nom_summ)
    elif operacion == "9":
        op_9(conexion,nom_years)
    elif operacion == "10":
        op_10(conexion)
    elif operacion == "0":
        print("Cerrando menú de operaciones.")
        seguir= False
    else:
        print("No existe la operación relacionada al valor ingresado.")

##se cierra la conexion y cursores cuando no hay mas solicitud de tareas
cursor.close()
conexion.close()
print("Conexion terminada.")