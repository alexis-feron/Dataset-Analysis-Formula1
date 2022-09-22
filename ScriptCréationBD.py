import pandas as pd
import psycopg2 as psy

co=None

try:
	co=psy.connect(host='berlin',
						database ='dbalferon',
						user='alferon',
						password ='')
	curs=co.cursor()

	# Suppression des tables si déjà existantes
	curs.execute('''DROP TABLE IF EXISTS constructor_result;''')
	curs.execute('''DROP TABLE IF EXISTS constructor_standing;''')
	curs.execute('''DROP TABLE IF EXISTS driver_standing;''')
	curs.execute('''DROP TABLE IF EXISTS lap_time;''')
	curs.execute('''DROP TABLE IF EXISTS pit_stops;''')
	curs.execute('''DROP TABLE IF EXISTS qualifying;''')
	curs.execute('''DROP TABLE IF EXISTS result;''')	
	curs.execute('''DROP TABLE IF EXISTS SprintResult;''')
	curs.execute('''DROP TABLE IF EXISTS races;''')
	curs.execute('''DROP TABLE IF EXISTS circuit;''')
	curs.execute('''DROP TABLE IF EXISTS drivers;''')
	curs.execute('''DROP TABLE IF EXISTS constructor;''')

	# Création de la table Constructor
	data=pd.read_csv(r'constructors.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE constructor(
						constructorId varchar(20),
						constructorRef varchar(20),
						name varchar(50),
						nationality varchar(20),
						PRIMARY KEY(constructorId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO constructor VALUES (%s ,%s ,%s ,%s);''',
						(row.constructorId , row.constructorRef , row.name , row.nationality))

	# Création de la table Drivers
	data=pd.read_csv(r'drivers.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE drivers(
						driverId varchar(20),
						driverRef varchar(20),
						number_ varchar(10),
						code varchar(3),
						forename varchar(50),
						surname varchar(50),
						dob varchar(20),
						nationality varchar(20),
						PRIMARY KEY(driverId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO drivers VALUES (%s ,%s ,%s ,%s,%s,%s,%s,%s);''',
						(row.driverId , row.driverRef , row.number , row.code,row.forename , row.surname , row.dob , row.nationality))

	# Création de la table Circuit
	data=pd.read_csv(r'circuits.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE circuit(
						circuitId varchar(20),
						circuitRef varchar(20),
						name varchar(50),
						location varchar(50),
						country varchar(20),
						alt varchar(20),
						PRIMARY KEY(circuitId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO circuit VALUES (%s ,%s ,%s ,%s,%s,%s);''',
						(row.circuitId , row.circuitRef , row.name , row.location, row.country , row.alt))

	# Création de la table Races
	data=pd.read_csv(r'races.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE races(
						raceId varchar(20),
						year numeric(20),
						round numeric(3),
						circuitId varchar(20) REFERENCES circuit,
						name varchar(50),
						date_ varchar(20),
						time_ varchar(20),
						PRIMARY KEY(raceId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO races VALUES (%s ,%s ,%s ,%s, %s ,%s , %s );''',
						(row.raceId , row.year , row.round ,row.circuitId ,row.name ,row.date, row.time))

	# Création de la table SprintResult
	data=pd.read_csv(r'sprint_results.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE SprintResult(
						resultId varchar(20),
						raceId varchar(20) REFERENCES races,
						driverId varchar(20) REFERENCES drivers,
						constructorId varchar(20) REFERENCES constructor,
						number_ varchar(10),
						grid varchar(20),
						positionOrder numeric(20),
						points numeric(10),
						laps numeric(3),
						time_ varchar(20),
						milliseconds varchar(10),
						PRIMARY KEY(resultId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO SprintResult VALUES (%s ,%s ,%s ,%s, %s ,%s, %s ,%s ,%s, %s,%s);''',
						(row.resultId , row.raceId , row.driverId , row.constructorId ,row.number ,row.grid ,row.positionOrder ,row.points, row.laps,row.time,row.milliseconds))

	# Création de la table Result
	data=pd.read_csv(r'results.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE result(
						resultId varchar(20),
						raceId varchar(20) REFERENCES races,
						driverId varchar(20) REFERENCES drivers,
						constructorId varchar(20) REFERENCES constructor,
						number_ varchar(10),
						grid varchar(20),
						positionOrder numeric(20),
						points numeric(10),
						laps numeric(3),
						time_ varchar(20),
						milliseconds varchar(10),
						fastestLapTime varchar(20),
						fastestLapSpeed varchar(20),
						PRIMARY KEY(resultId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO result VALUES (%s ,%s ,%s ,%s, %s ,%s, %s ,%s ,%s, %s,%s,%s,%s);''',
						(row.resultId , row.raceId , row.driverId , row.constructorId ,row.number ,row.grid ,row.positionOrder ,row.points, row.laps,row.time,row.milliseconds, row.fastestLapTime, row.fastestLapSpeed))

	# Création de la table Qualifying
	data=pd.read_csv(r'qualifying.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE qualifying(
						qualifyId varchar(20),
						raceId varchar(20) REFERENCES races,
						driverId varchar(20) REFERENCES drivers,
						constructorId varchar(20) REFERENCES constructor,
						number_ varchar(10),
						position varchar(20),
						q1 varchar(20),
						q2 varchar(20),
						q3 varchar(20),
						PRIMARY KEY(qualifyId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO qualifying VALUES (%s ,%s ,%s ,%s, %s ,%s , %s,%s,%s );''',
						(row.qualifyId , row.raceId , row.driverId ,row.constructorId ,row.number ,row.position, row.q1,row.q2,row.q3))

	# Création de la table Pit_stops
	data=pd.read_csv(r'pit_stops.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE pit_stops(
						raceId varchar(20) REFERENCES races,
						driverId varchar(20) REFERENCES drivers,
						stop numeric(10),
						lap numeric(3),
						time_ varchar(20),
						duration varchar(10),
						milliseconds varchar(10),
						PRIMARY KEY(raceId,driverId,stop)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO pit_stops VALUES (%s ,%s ,%s ,%s, %s ,%s , %s );''',
						(row.raceId , row.driverId ,row.stop ,row.lap ,row.time, row.duration,row.milliseconds))					

	# Création de la table Lap_time
	data=pd.read_csv(r'lap_times.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE lap_time(
						raceId varchar(20) REFERENCES races,
						driverId varchar(20) REFERENCES drivers,
						lap numeric(3),
						position numeric(3),
						time_ varchar(20),
						milliseconds varchar(10),
						PRIMARY KEY(raceId,driverId,lap)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO lap_time VALUES (%s ,%s ,%s ,%s, %s ,%s );''',
						(row.raceId , row.driverId ,row.lap ,row.position,row.time, row.milliseconds))					
	
	# Création de la table Driver_standing
	data=pd.read_csv(r'driver_standings.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE driver_standing(
						driverStandingId varchar(20),
						raceId varchar(20) REFERENCES races,
						driverId varchar(20) REFERENCES drivers,
						points numeric(10),
						position numeric(20),
						wins numeric(10),
						PRIMARY KEY(driverStandingId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO driver_standing VALUES (%s ,%s ,%s ,%s, %s ,%s);''',
						(row.driverStandingsId , row.raceId , row.driverId , row.points ,row.position , row.wins))
	
	# Création de la table Constructor_standing
	data=pd.read_csv(r'constructor_standings.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE constructor_standing(
						constructorStandingId varchar(20),
						raceId varchar(20) REFERENCES races,
						constructorId varchar(20) REFERENCES constructor,
						points numeric(10),
						position numeric(20),
						wins numeric(10),
						PRIMARY KEY(constructorStandingId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO constructor_standing VALUES (%s ,%s ,%s ,%s, %s ,%s);''',
						(row.constructorStandingsId , row.raceId , row.constructorId , row.points ,row.position , row.wins))

	# Création de la table Constructor_result
	data=pd.read_csv(r'constructor_results.csv')
	df=pd.DataFrame(data)
	df2=df.drop_duplicates()
	curs.execute('''CREATE TABLE constructor_result(
						constructorResultId varchar(20),
						raceId varchar(20) REFERENCES races,
						constructorId varchar(20) REFERENCES constructor,
						points numeric(10),
						PRIMARY KEY(constructorResultId)
					);''')
	for row in df2.itertuples():
		curs.execute('''INSERT INTO constructor_result VALUES (%s ,%s ,%s ,%s);''',
						(row.constructorResultsId , row.raceId , row.constructorId , row.points))


	co.commit()
	curs.close()

except(Exception,psy.DatabaseError) as error :
	print(error)

finally :
	if co is not None:
		co.close ()