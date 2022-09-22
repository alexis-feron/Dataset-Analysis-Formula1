import pandas as pd
import psycopg2 as psy
import matplotlib.pyplot as plt

connec = None
pays=['Britanniques','Americain','Italien','Francais','Allemand','Brésilien','Argentin','Sud Africain','Suisse','Belge']
constructeurs=['Ferrari','McLaren','Mercedes','Williams','Red Bull','Lotus','Renault','Benetton','Tyrell','Brabham']

try:
    connec = psy.connect(host='berlin',
                        database ='dbalferon',
                        user='alferon',
                        password ='')

    # Nombre de victoire pilote
    datafr = pd.read_sql('''Select d.surname, count(1) nombre_de_victoires from result r, drivers d where d.driverId=r.driverId and positionOrder=1 group by d.surname,d.forename order by 2 desc ''', con=connec)
    fig = datafr.head(15).plot.bar(x='surname', y='nombre_de_victoires', color=['#715C91','#74B7A7','#E3D6B4','#D5B089','#916B8B','#BB7B73']) # Generation du graphique
    #plt.show() # Affichages

    # Nombre de victoires constructeur
    datafr = pd.read_sql('''Select c.name, count(1) nombre_de_victoires from result r, constructor c where c.constructorId=r.constructorId and positionOrder=1 group by c.name order by 2 desc''', con=connec)
    fig = datafr.head(10).plot.bar(x='name', y='nombre_de_victoires', color=['#a10100','#da1f05','#f33c04','#fe650d','#ffc11f','#fff75d']) # Generation du graphique
    fig=datafr.head(10).plot(y=1, legend=0,labels=constructeurs, kind='pie') # Generation du graphique
    #plt.show() # Affichage

    # Circuits les plus utilisés
    datafr = pd.read_sql('''Select c.name, count(1) nombre_de_courses from races r,circuit c where r.circuitId=c.circuitId group by c.name order by 2 desc''', con=connec)
    fig = datafr.head(15).plot.bar(x='name', y='nombre_de_courses',color=['#1E90FC','#246FF0', '#2A4EE5', '#302DD9', '#360CCD']) # Generation du graphique
    #plt.show() # Affichage

    # Nombre de courses par années
    datafr = pd.read_sql('''Select cast(year as int), max(round) nombre_de_courses from races group by 1 order by 1 desc''', con=connec)
    fig = datafr.head(30).plot(x='year', y='nombre_de_courses',color='#715C91') # Generation du graphique
    #plt.show() # Affichage

    # Nombre de courses des pilotes
    datafr = pd.read_sql('''Select d.surname, count(1) nombre_de_courses from drivers d,result r where d.driverId=r.driverId group by d.surname,d.forename order by 2 desc''', con=connec)
    fig = datafr.head(15).plot.bar(x='surname', y='nombre_de_courses', color=['#9DDD8F','#cedcc7','#7EB2DF','#E0F3D2']) # Generation du graphique
    #plt.show() # Affichage

    # Nombre de pilotes par pays
    datafr = pd.read_sql('''Select nationality, count(1) nombre_de_pilotes_par_pays from drivers group by nationality order by 2 desc''', con=connec)
    fig=datafr.head(10).plot(y=1, legend=0, labels=pays, kind='pie') # Generation du graphique
    fig = datafr.head(10).plot.bar(x='nationality', y='nombre_de_pilotes_par_pays',color=['#1E90FC','#246FF0', '#2A4EE5', '#302DD9', '#360CCD']) # Generation du graphique
    plt.show() # Affichage

except(Exception, psy.DatabaseError) as error :
    print(error)

finally:
    if connec is not None:
        connec.close()