import pywren_ibm_cloud as pywren
import numpy as np
import sys
from cos_backend import COSBackend
import time

global x,y,z
global result
global porcio_basica_fila, residu_fila, porcio_basica_col, residu_col
global num_w_m1,num_w_m2, llista
global mat1, mat2

def crea_matrius(i):
	matriu1=np.random.randint(9,size=(x,y))
	matriu2=np.random.randint(9,size=(y,z))
	return(matriu1,matriu2)
	
def mul_mat(i):
	"""
	result=np.zeros((y,z))
	
	for i in range(len(mat1)):
		for j in range(len(mat2[0])):
			for k in range(len(mat2)):
				result[i][j] += mat1[i][k] * mat2[k][j]
	"""
	result=np.matmul(mat1,mat2)
	return(result)
		
def inicialitza(i):
	
	
	
	ibmcf = pywren.ibm_cf_executor()
	
	if(num_w==1):
		ibmcf.call_async(funcio_unica,4)
	else:
		minim=0	
		if(residu_fil):
			ibmcf.call_async(funcio_residu_fila,4)
			cont=1
			minim=residu_fil+1
		else:
			cont=0
			
			
		while(cont<num_w_m1):
			
			ibmcf.call_async(funcio_normal_fila,minim)
			minim+=porcio_basica_fil
			cont+=1
		
		minim=0	
		if(residu_col):
			ibmcf.call_async(funcio_residu_col,4)
			cont=1
			minim=residu_col+1
		else:
			cont=0
		
		
		while(cont<num_w_m2):
			
			#print("****"+str(minim)+"\n")
			ibmcf.call_async(funcio_normal_col,minim)
			minim+=porcio_basica_col
			cont+=1
	
	"""for a in range(x):
		
		data=cos.get_object('sd-ori-un-buen-cubo', 'fila'+str(a+1)+'.txt')
		print(data.decode())
		
	for a in range(z):
		
		data=cos.get_object('sd-ori-un-buen-cubo', 'col'+str(a+1)+'.txt')
		print(data.decode())"""
		
def funcio_residu_fila(minim):	#minim és la fila on comença a tractar
	cos=COSBackend()
	minim=0
	maxim=residu_fil+1

	llista=list()
	while minim<maxim:	#minim és la fila actual a tractar
		dada=""
		for j in range(y):	#j és la columna que estem tractant actualment
			#print(type(mat1[minim][j]))
			dada=dada+str(mat1[int(minim)][int(j)])+","
		
		dada=dada[:-1]
		dada=dada.encode()
		cos.put_object('sd-ori-un-buen-cubo', 'fila'+str(int(minim)+1)+'.txt', dada)
		
		minim+=1
		
	#pujar llista al cloud com a fitxer


def funcio_normal_fila(minim):	#minim és la fila on comença a tractar
	cos=COSBackend()
	
	maxim=int(minim)+porcio_basica_fil
	while minim<maxim:	#minim és la fila actual a tractar
		dada=""
		for j in range(y):	#j és la columna que estem tractant actualment
			#print(type(mat1[minim][j]))
			dada=dada+str(mat1[int(minim)][int(j)])+","
		
		dada=dada[:-1]
		dada=dada.encode()
		cos.put_object('sd-ori-un-buen-cubo', 'fila'+str(int(minim)+1)+'.txt', dada)
		minim+=1
	
	#pujar llista al cloud com a fitxer

def funcio_residu_col(minim):	#minim és la fila on comença a tractar
	cos=COSBackend()
	minim=0
	maxim=residu_col+1

	llista=list()
	while minim<maxim:	#minim és la fila actual a tractar
		dada=""
		for j in range(y):	#j és la columna que estem tractant actualment
			#print(type(mat1[minim][j]))
			dada=dada+str(mat2[int(j)][int(minim)])+","
		
		dada=dada[:-1]
		dada=dada.encode()
		cos.put_object('sd-ori-un-buen-cubo', 'col'+str(int(minim)+1)+'.txt', dada)
		
		minim+=1
		
	#pujar llista al cloud com a fitxer


def funcio_normal_col(minim):	#minim és la fila on comença a tractar
	cos=COSBackend()
	
	maxim=int(minim)+porcio_basica_col
	llista=list()
	while minim<maxim:	#minim és la fila actual a tractar
		dada=""
		for j in range(y):	#j és la columna que estem tractant actualment
			#print(type(mat1[minim][j]))
			dada=dada+str(mat2[int(j)][int(minim)])+","
		
		dada=dada[:-1]
		dada=dada.encode()
		cos.put_object('sd-ori-un-buen-cubo', 'col'+str(int(minim)+1)+'.txt', dada)
		minim+=1

	#pujar llista al cloud com a fitxer

def funcio_unica(minim):
	
	cos=COSBackend()
	minim=0
	maxim=x
	
	while minim<maxim:	#minim és la fila actual a tractar
		dada=""
		for j in range(y):	#j és la columna que estem tractant actualment
			#print(type(mat1[minim][j]))
			dada=dada+str(mat1[int(minim)][int(j)])+","
		
		dada=dada[:-1]
		dada=dada.encode()
		cos.put_object('sd-ori-un-buen-cubo', 'fila'+str(int(minim)+1)+'.txt', dada)
		minim+=1
	
	maxim=z
	minim=0
	
	while minim<maxim:	#minim és la fila actual a tractar
		dada=""
		for j in range(y):	#j és la columna que estem tractant actualment
			#print(type(mat1[minim][j]))
			dada=dada+str(mat2[int(j)][int(minim)])+","
		
		dada=dada[:-1]
		dada=dada.encode()
		cos.put_object('sd-ori-un-buen-cubo', 'col'+str(int(minim)+1)+'.txt', dada)
		minim+=1
	

def funcio_map(k):
	
	cos=COSBackend()
	"""f=0
	j=0"""

	
	#for i in range(len(iterdata)):
	#cont=0
	k=k.split(" ")
	cont=0
	dada=''
	
	for a in range(len(k)//int(2)):	
		i=k[cont]
		j=k[cont+1] 
		cont+=2
		fil='fila'+str(int(i)+1)+'.txt'
		col='col'+str(int(j)+1)+'.txt'
		 
		fila=cos.get_object('sd-ori-un-buen-cubo', fil)
		columna=cos.get_object('sd-ori-un-buen-cubo', col)
		fila=fila.decode()
		columna=columna.decode()
	
		fila=fila.split(",")
		columna=columna.split(",")	
		
		acum=0
		for b in range(len(fila)):
			acum+=int(fila[b])*int(columna[b])
		
		dada+=str(i)+" "+str(j)+' '+str(acum)+' '
	
	dada=dada[:-1]	
	dada=dada.encode()	
	cos.put_object('sd-ori-un-buen-cubo', 'worker'+k[len(k)-1]+'.txt', dada)
	return(k[len(k)-1])

def funcio_reduce(results):
	cos=COSBackend()
	mat_result=np.zeros(shape=(x,z))
	
	for m in range(len(results)):
			
			valor=cos.get_object('sd-ori-un-buen-cubo', 'worker'+results[m]+'.txt')
			valor=valor.decode()
			cont=0
			valor=valor.split(" ")
			for n in range(len(valor)//3):
				i=int(valor[cont])
				j=int(valor[cont+1])
				res=valor[cont+2]
				cont+=3
				
				mat_result[i][j]=res
	
	return(mat_result)
	
			
		
if __name__ == '__main__':
	
	start_time=time.time()
	
	cos=COSBackend()
	x=int(sys.argv[1])
	y=int(sys.argv[2])
	z=int(sys.argv[3])
	num_w=int(sys.argv[4])
	
	iterdata=[]

	pos_per_worker=(x*z)//num_w
	i=0
	j=0
	for k in range(num_w):
		data =''
		for a in range(pos_per_worker):
			data+=str(i)+' '+str(j)+' '
			if(j==(z-1)):
				i+=1
				j=0
			else:
				j+=1
		data=data[:-1]
		iterdata.append(data)
	
	res_work=(x*z)%num_w
	
	for k in range(res_work):
		data=iterdata[k]
		data+=' '+str(i)+' '+str(j)
			
		if(j==(z-1)):
			i+=1
			j=0
		else:
			j+=1
		iterdata[k]=data
		
	
	for k in range(num_w):
		data=iterdata[k]
		data+=' '+str(k)
		iterdata[k]=data
	"""for i in range(len(iterdata)):
		print(iterdata[i].split(" "))
		print("\n")	"""
	
	if(num_w>x): num_w=x	#Afegir cas per col
	
	
	num_w_m1=num_w//2
	num_w_m2=num_w//2
	if(num_w%2): num_w_m1+=1
	
	
	if (num_w_m1>1):	
		residu_fil=x%(num_w_m1-1)
		porcio_basica_fil=(x-residu_fil)//(num_w_m1-1)
		if (residu_fil is 0):
			porcio_basica_fil=x//num_w_m1
	else:
		porcio_basica_fil=x
		residu_fil=0
			
	if (num_w_m2>1):
		residu_col=z%(num_w_m2-1)
		porcio_basica_col=(z-residu_col)//(num_w_m2-1)
		if (residu_col is 0):
			porcio_basica_col=z//num_w_m2
	else: 
		porcio_basica_col=z
		residu_col=0
	
	if(num_w_m1*porcio_basica_fil+residu_fil is not x):residu_fil=x-num_w_m1*porcio_basica_fil+porcio_basica_fil	
	
	if(num_w_m2*porcio_basica_col+residu_col is not z):residu_col=z-num_w_m2*porcio_basica_col+porcio_basica_col
	
		
	

	mat1,mat2=crea_matrius(4)
	inicialitza(4)
		
	#MULTIPLICACIÓ NO DISTRIBUIDA
	
	
	

	#sd_map()
	"""data=cos.get_object('sd-ori-un-buen-cubo', 'map_m1.txt')
	print(data)"""
		
	
	
	#MULTIPLICACIÓ DISTRIBUIDA
	
	
	
	ibmcf = pywren.ibm_cf_executor()
	#ibmcf.call_async(inicialitza,4)	#Crida de l'acció de crear matrius a IBM

									#Obtenim les matrius a multiplicar
	"""print(mat1)
	print("\n")
	print(mat2)
	print("\n")"""
										#Bucles per pujar a IBM les files de la
										#primera matriu i les columnes de la segona
										#perquè els workers puguin agafar els trossos
	"""for i in range(x):					#de les dades que els hi correspon

		fila=(str(mat1[i]).lstrip('[').rstrip(']'))
		cos.put_object('sd-ori-un-buen-cubo', 'fila'+str(i+1)+'.txt', fila)
		data=cos.get_object('sd-ori-un-buen-cubo', 'fila'+str(i+1)+'.txt')

	
	for i in range(z):
		
		col=""
		for j in range(y): col=col+(str(mat2[j][i]))+" "
		
		cos.put_object('sd-ori-un-buen-cubo', 'col'+str(i+1)+'.txt', col)
		data=cos.get_object('sd-ori-un-buen-cubo', 'col'+str(i+1)+'.txt')
	"""
	#ibmcf.call_async(mul_mat,4)

	#print(ibmcf.get_result())
	"""
	print("\n")
	print(iterdata)
	for i in range(x*z):
		print(funcio_map(i))"""

	"""for i in range(len(iterdata)):
		print(iterdata[i].split(" "))
		print("\n")"""
	

	print("\nMètode NumPy:\n")
	print(mul_mat(4))
	
	ibmcf.map_reduce(funcio_map,iterdata,funcio_reduce, reducer_wait_local=True)
	print("\nMètode SD_Tasca1:\n")
	print(ibmcf.get_result())
	
	print("--- %s seconds ---" % (time.time() - start_time))
	#print(cos.get_object('sd-ori-un-buen-cubo', 'mat_result.txt').decode())
