#include <stdio.h>
#include <time.h>
#include <pthread.h>

pthread_mutex_t acquire_release_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t write_lock = PTHREAD_MUTEX_INITIALIZER;

int num_with_shared_lock = 0;
int num_with_excl_lock = 0;


void acquire_shared_lock() {
	while (1) {
       pthread_mutex_lock(&acquire_release_lock);
       int x = pthread_mutex_trylock(&write_lock);
       // if the pthread_mutex_trylock returns 0, if means we successfully acquired the 
       // write_lock. That is to say now the write_lock is not helded by the acquire_exclusive_lock().
       if (x != 0) {
        //else the write_lock
         //printf("%s\n","write_lock helded" );
         pthread_mutex_unlock(&acquire_release_lock);
         continue;
       }
       // Once we have make sure that write_lock() has not been helded by acquire_exclusive_lock(). 
       // We should release it right away.
       pthread_mutex_unlock(&write_lock);
       //printf("%s\n","write_lock NOT helded");
       num_with_shared_lock++;
       //printf("acquire: %d\n",num_with_shared_lock);
       pthread_mutex_unlock(&acquire_release_lock);
       break;
     }

  }

void release_shared_lock() {
	pthread_mutex_lock(&acquire_release_lock);
     num_with_shared_lock--;
     //printf("release: %d\n",num_with_shared_lock);
  pthread_mutex_unlock(&acquire_release_lock);
}

void acquire_exclusive_lock() {
	int has_lock = 0;
     while (! has_lock) {
       pthread_mutex_lock(&acquire_release_lock);
       if (num_with_shared_lock == 0 && num_with_excl_lock == 0) {
         num_with_excl_lock++;
         //printf("%s: %d\n","acquire exclusive lock",num_with_excl_lock );
         pthread_mutex_lock(&write_lock);
         has_lock = 1;
       }
       pthread_mutex_unlock(&acquire_release_lock);
       if (! has_lock) {  // delay before trying again for the lock
         struct timespec ten_milliseconds = {0, 10000000};
         //printf("%s\n","sleep.");
         nanosleep(&ten_milliseconds, NULL);
       }
     }
}

void release_exclusive_lock() {
	pthread_mutex_lock(&acquire_release_lock);
     num_with_excl_lock--;
     pthread_mutex_unlock(&write_lock);
     //printf("%s: %d\n","exclusive lock released the write_lock",num_with_excl_lock);
  pthread_mutex_unlock(&acquire_release_lock);
}
