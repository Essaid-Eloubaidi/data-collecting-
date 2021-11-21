#debut de programme
from ftplib import FTP
import datetime
#pour pouvoir utiliser la notion de temps
from datetime import date
import csv
import mysql.connector
#permet de se connecter a la base de donnee MySql
import sys
#Ce module fournit un acces a certaines variables utilisees



now=date.today().strftime("%d/%m/%Y")
#retourne la date actuelle
print("la date d'aujourd'hui est : {} ".format(now))


#connexion au serveur FTP
try:
    ftp= FTP("ftps4.us.freehostia.com")
    ftp.login("aboben","gepghennioui")
    print("la connexion au serveur est effectuee avec succee")
except:
    print("la connexion au serveur a ete echouee")    


#connexion au base de donnees locale
print("trying to be connected with the database :")
bdd=mysql.connector.connect(host="localhost",user="user",passwd="iresen2019",database="files_databases",charset="utf8")
if bdd:
    print("succesfull connection to the database")
else:
    print("there is an ERROR when connecting to the database")


#requete pour determiner la plus grande date des fichiers venant au serveur FTP
query1="select max(temp) from borne_info"
q=bdd.cursor()
q.execute(query1)
max_time=q.fetchone() 
print(max_time)

#requete pour parcourir les noms des fichiers dans la base de donnees
#et les comparer avec le nouveau fichier afin d'eviter la repetion
query2="select Name_of_file from borne_info"
q=bdd.cursor()
q.execute(query2)
names=q.fetchall()
print(len(names))

file_list=[]
ftp.retrlines("NLST",file_list.append)
#la fonction retrlines retourne les noms des fichiers contenus dans le serveur FTP
print(file_list)
i=0
j=len(file_list)
print(j)
while(i<j):
    print(i)
    if(file_list[i][-4:]==".csv"):
        #verification de l'extension
        print("ce fichier est effictivement d'extension .csv")
        temp=file_list[i][35:43]
        #temp est la date la plus grande parmis les dates des fichiers qui sont deja stockes dans la base de donnees
        print(temp)
        Nom=file_list[i][0:43]
        print(Nom)
        #convertion d'une chaine de caract�re(qui est la date du fichier) au format standard de la date
        file_time=datetime.datetime.strptime(temp,'%Y%m%d') 
        print(file_time)
        d=0

        #v�rification des noms pour les bornes qui �taient d�connet�s
        for name in names:
            if(Nom!=name[0]):
                d+=1
            else:
                break
        if(d==len(names)):
            #la variable names contient les noms de tous les fichiers qui sont deja dans la base de donnees
            print("OK")
            charH='EVC1S22P4E4ED418'
            charM='000000'
            charL='0000000'
            #charH,charM et charL sont les portions des noms qui sont communs pour tous les fichiers
            var1=file_list[i][16:19]
            var2=file_list[i][25:27]
            N=open(charH+var1+charM+var2+charL+'_'+temp+'-00_00_DLog.csv',"wb") 


            print("le serveur est ouvert pour la lecture des fichiers :")
            ftp.retrbinary("RETR " +charH+var1+charM+var2+charL+'_'+temp+'-00_00_DLog.csv',N.write,8*1024)
            #la fonction retrbinary c'est pour Recuperer les fichier en mode de transfert binaire
            #la valeur 8192 : specifie la taille de bloc maximale a lire sur l'objet socket de bas niveau
            #cree pour effectuer le transfert
            N.close()

            #lecture du contenu du fichier
            file=open(charH+var1+charM+var2+charL+'_'+temp+'-00_00_DLog.csv')
            dataReader=csv.reader(file,delimiter=';')
            file=list(dataReader)
            #on selectionne juste la ligne utile car le fichier contient une autre ligne vide
            row=file[1]
            print(row)
            print("salamo Alikom")
            print("row to insert in the database")
            line=row
            cursor=bdd.cursor()


            #la requete responsable de l'insertion automatique des donnees
            query="""INSERT INTO borne_info(CDR_ID,CS_ID,socketoutlet_ID,Transaction_ID,UID,Type_of_charge,
            Start_Datetime,End_Datetime,Energy_KWh,Socket_Type,Duration,Comment,temp,Name_of_file) VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            #la donnee
            data=(line[0],line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],
            line[11],temp,Nom)
            #l'insertion dans notre database
            cursor.execute(query,data)
            bdd.commit()
            i+=1
        else:
            print("NOT OK")
            i+=1
    else:
        i+=1


#End de programme
