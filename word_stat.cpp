#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <cstdlib>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <vector>
#include <stdexcept>
char map_pool[100][100];
int p_ind;
int size;
int flag=0;;
int flag_r=0;
int flag_s=0;
int size_l;
int size_k;

int exit_map_update=0;
int exit_mapper=0;
int exit_reducer=0;
int exit_summarizer=0;
sem_t emptyBuffers;
sem_t fullBuffer;
pthread_mutex_t mutex,mutex1,mutex2;

pthread_cond_t condc, condp;
pthread_cond_t conds,condr;
pthread_cond_t condc1,condr1;
char reducer_pool[100][100];
char summarizer_pool[100][100];

void* mapper_pool_updater(void *filename)
{
	    char* name;
	    p_ind=0;
	    size=0;
	    name = (char*)filename;
	    std::ifstream file;
            char map_pool_temp[100][100];
	    int i=0;
	    int j;
	    file.open(name);
	    char key[100];
	    file>>key;
            while(file.good()){
	    
            strcpy(map_pool_temp[i],key);
	    file>>key;
	    if(key[0] > map_pool_temp[i][0])
		{       
			
		        pthread_mutex_lock(&mutex);
		        if(flag != 0)
			pthread_cond_wait(&condp, &mutex);
			
                        size=(i-p_ind)+1;
			for(j=0;j<size;j++)
				{
					strcpy(map_pool[j],map_pool_temp[p_ind+j]);
					
				}
			flag=1;
			p_ind=i+1;
			pthread_cond_signal(&condc);
			pthread_mutex_unlock(&mutex);
			
		}
	    i++;
			      }
	 
	
	   		pthread_mutex_lock(&mutex);
		        if(flag != 0)
			{pthread_cond_wait(&condp, &mutex);}
                        size=(i-p_ind);
			for(j=0;j<size;j++)
				{
					strcpy(map_pool[j],map_pool_temp[p_ind+j]);
					
					
				}
			flag=1;
			
			pthread_cond_signal(&condc);
			pthread_mutex_unlock(&mutex); 


	    exit_map_update=1;

	   
}


void* mapper(void *param)
{
	
	while(1){	
       int m=0;
       
	char red_buff[100][100];
       pthread_mutex_lock(&mutex);
       //if(flag !=1 && flag_r !=0)
       //{pthread_cond_wait(&condc, &mutex);}
       if(flag !=1)
	pthread_cond_wait(&condc, &mutex);
	pthread_mutex_lock(&mutex1);
	if(flag_r !=0)
	pthread_cond_wait(&condc1,&mutex1);
       for(m=0;m<size;m++)
	{	
			
	strcpy(reducer_pool[m],"(");
	strcat(reducer_pool[m],map_pool[m]);
	strcat(reducer_pool[m],",");
	strcat(reducer_pool[m],"1");
	strcat(reducer_pool[m],")");
	//d::cout<<"mapper Output"<<std::endl<<reducer_pool[m];
	}
	
	size_l=size;
	
	flag=0;
	flag_r =1;
	
	pthread_cond_signal(&condr);
	pthread_cond_signal(&condp);
	pthread_mutex_unlock(&mutex1);
	pthread_mutex_unlock(&mutex);}
	
	
}	



void* reducer(void *param)

{
	while(1){

	std::vector <std::string> hold;
	int flag_l;
	int i;
	int j;
	int pos;
	int num[50];
	std::string output;  
	std::string key_red_sub;
	int size_red;
	try {
	  for ( j = 0; j < 50; j++) 
		{
			num[j] = 1;
		}
	std::string key_red;
        pthread_mutex_lock(&mutex1);
        
	if(flag_r !=1)
	pthread_cond_wait(&condr,&mutex1);

	for(i=0;i < size_l;i++)
	{
				
		key_red=reducer_pool[i];
		pos = key_red.find(",");
		 key_red_sub=key_red.substr(1,pos-1);
		 size_red = hold.size();
		flag_l=0;
		for(j = 0; j < size_red; j++)
  			{
  			 if(hold[j]==key_red_sub)
  				{
  				  num[j]++;
				  flag_l=1;
								
  				}
  							
  			}

		if(flag_l == 0)
		{
			hold.push_back(key_red_sub);
		}
		
	}
		pthread_mutex_lock(&mutex2);
	        if(flag_s !=0)
	        pthread_cond_wait(&condr1,&mutex2);			
		for (unsigned int i = 0; i < hold.size(); i++)
			{
			     	char buff[10];
				sprintf(buff, "%d", num[i]);
				
			        strcpy(summarizer_pool[i],"(");
				strcat(summarizer_pool[i],hold[i].c_str());
				strcat(summarizer_pool[i],",");
				strcat(summarizer_pool[i],buff);
				strcat(summarizer_pool[i],")");
				
			
						
			}	
			size_k=hold.size();				
			hold.clear();}
	catch (const std::out_of_range& oor) {
    
                                                            }	

	flag_r=0;
	flag_s=1;
	pthread_cond_signal(&condc1);
	pthread_cond_signal(&conds);
	pthread_mutex_unlock(&mutex2);
	pthread_mutex_unlock(&mutex1);
		

				  
	}
	
}




void* word_count_writer(void* param)
{

	while(1){
		
		int m;
		pthread_mutex_lock(&mutex2);
                if(flag_s !=1)
                pthread_cond_wait(&conds, &mutex2);
		std::ofstream outfile;
		outfile.open("Output.txt", std::ios_base::app);
		for(m=0;m<size_k;m++)
		{	
			
			outfile << summarizer_pool[m]<<std::endl; 
		
		}
		flag_s=0;
		pthread_cond_signal(&condr1);
		pthread_mutex_unlock(&mutex2);
		

		}

	

}



using namespace std;

int main()
{
  	 int i;
	 pthread_t mapper_t[10];
	 pthread_t reducer_t[10];
	 int NumberofM_Threads;
	 int NumberofR_Threads;
	 pthread_cond_init(&condc, NULL);		/* Initialize consumer condition variable */
         pthread_cond_init(&condp, NULL);		/* Initialize producer condition variable */
	pthread_cond_init(&conds, NULL);		/* Initialize consumer condition variable */
         pthread_cond_init(&condr, NULL);		/* Initialize producer condition variable */
	pthread_cond_init(&condr1, NULL);
	pthread_cond_init(&condc1, NULL);

	 char temp[10];
         char inputfile[100];
	 sem_init(&emptyBuffers,0,10);
         sem_init(&fullBuffer,0,0);
	 pthread_mutex_init(&mutex,NULL);
	 pthread_mutex_init(&mutex1,NULL);
	 pthread_mutex_init(&mutex2,NULL);
	 
	 cout<<"Enter the Filename"<<endl;
	 cin>>inputfile;
	 cout<<"Enter the number of mapper Threads"<<endl;

	 cin>>temp;        
         NumberofM_Threads=atoi(temp);
	 cout<<"Enter the Number of Reducer Threads"<<endl;
	 cin>>temp;
     	 NumberofR_Threads=atoi(temp);
	 
	 pthread_t map;
	 pthread_t summary;
	 
	 pthread_create(&map, NULL,mapper_pool_updater,(void*)inputfile);
	 
	for (i = 0; i < NumberofM_Threads; i++) {  // create Mappers
             pthread_create(&mapper_t[i], NULL,mapper,NULL);}
	for (i = 0; i < NumberofR_Threads; i++) {  // create Reducers
             pthread_create(&reducer_t[i], NULL,reducer,NULL);}
	pthread_create(&summary, NULL,word_count_writer,NULL);
	 

	
	sleep(3);
        return 0;

}
