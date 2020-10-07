import numpy as np
import argparse
import os
import re
import time
import pickle
from mpi4py import MPI
import time


mpi_comm  = MPI.COMM_WORLD
status    = MPI.Status()
comm_size = mpi_comm.Get_size()
rank      = mpi_comm.Get_rank()
start_time = time.time()

regex = re.compile('[+@_!#$%^&*()<>?/\|}{~:]')


ap = argparse.ArgumentParser()
ap.add_argument("-s", "--seed", nargs='+',  required=True, help="Seed Files")
ap.add_argument("-c", "--chr" , nargs='+',  required=True, help="Chromosomes Files")
args = vars(ap.parse_args())

## Master Process ##
if rank==0:
    all_results = {}

    # A function to revieve results as they arrive from workers
    def receive_results(chr_name):
        result = {}
        worker_count = 0
        while True:   

            msg = mpi_comm.recv(source=worker_count+1, status=status) 
            print ("Master received a msg from Worker {} with tag: {}".format(status.Get_source(), status.Get_tag()))
            if status.Get_tag()==0:                         
                worker_count+=1
                print ("Master: Worker {} finshed a task".format(status.Get_source()))
                if worker_count==comm_size-1: break         
            elif status.Get_tag()==1:                      
                print('Here-------------')
                if result.has_key(msg[0]):                  
                    result[msg[0]]+=msg[1]
                else:
                    result[msg[0]] = msg[1]
        all_results[chr_name] = result
        #print("Result", all_results)
       

        with open("Menavlikar_hw2.txt", 'w') as f:
            for key, value in all_results.iteritems():
                #print('Key',key)
                f.write('%s  %s\n' % (key, value)) 
#                for k in value.keys():
#                    f.write('%s %s %s\n', (key, k, value.get(k)))
        
        print("--- Execution Time : %s seconds ---" % (time.time() - start_time))

        

    for seed_file in args['seed']:                  # Interating through the Seed files
        with open(seed_file, 'rb') as seedF:       
            seed = [x[:-2].upper() for x in seedF if regex.search(x) == None]

        print ("Master: Seed {} started".format(seed_file))

        for chr_file in args['chr']:                # Interating through the Chromosomes
            with open(chr_file, 'rb') as chrF:     
                chr_name = chrF.readline()[1:-2]                              
                chr = chrF.read().replace('\n','').upper().replace('N', '') 
                print(len(chr))
        

            totalDataLines = len(seed)
            totalChunks = comm_size - 1
            linesPerChunk = int(totalDataLines / totalChunks)
            chunkDivision = []
            for j in range(0, linesPerChunk * (totalChunks - 1), linesPerChunk):
                chunkDivision.append(seed[j:j + linesPerChunk])
                # print(chunkDivision)
            if j < totalDataLines:
                chunkDivision.append(seed[j:])

            for i in xrange(1,comm_size):
               
                mpi_comm.send([chunkDivision[i-1], chr], dest=i, tag=1)

            receive_results(chr_name)                                      
            print ("Master:---> completed Chromosome {} completed".format(chr_file))
        print ("Master: Seed {} finished".format(seed_file))
        print ("------------------------")

    
    for r in range(1,comm_size,1):
       
        mpi_comm.send([None,None], dest=r, tag=0)
    print ("Master finished work and is leaving... Bye!")

    



## Worker Processes ##
else:
    while True:                                
        msg = mpi_comm.recv(source=0, status=status)
        seed, chr = msg
        if status.Get_tag() == 0:      
            print ("Worker {} received a termination tag from Master... Bye!".format(rank))
            break   
        print ("Worker {} received a msg from Master!".format(rank))
        genomes = {}
        for g in seed:                         
            if genomes.has_key(g):
                genomes[g]+=1
            else:
                genomes[g]=1
        
        #print(genomes)
       
        for g, count in genomes.iteritems():    # Search for the indecies of the genomes in the Chromosome
            #print("Here--------")
            indicies = [i.start() for i in re.finditer(g, chr)]
            
            for i in indicies:                   
                mpi_comm.send([i, count], dest=0, tag=1)

        #print("Matched indices: ",indicies)

        print ("Worker {} finished its task and ready for the next!".format(rank))
        mpi_comm.send(None, dest=0, tag=0)      
