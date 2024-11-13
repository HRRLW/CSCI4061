#include "../include/server.h"

// /********************* [ Helpful Global Variables ] **********************/
int num_dispatcher = 0; //Global integer to indicate the number of dispatcher threads   
int num_worker = 0;  //Global integer to indicate the number of worker threads
FILE *logfile;  //Global file pointer to the log file
int queue_len = 0; //Global integer to indicate the length of the queue

/* TODO: Intermediate Submission
  TODO: Add any global variables that you may need to track the requests and threads
  [multiple funct]  --> How will you track the p_thread's that you create for workers?
  [multiple funct]  --> How will you track the p_thread's that you create for dispatchers?
  [multiple funct]  --> Might be helpful to track the ID's of your threads in a global array
  What kind of locks will you need to make everything thread safe? [Hint you need multiple]
  What kind of CVs will you need  (i.e. queue full, queue empty) [Hint you need multiple]
  How will you track the number of images in the database?
  How will you track the requests globally between threads? How will you ensure this is thread safe? Example: request_t req_entries[MAX_QUEUE_LEN]; 
  [multiple funct]  --> How will you update and utilize the current number of requests in the request queue?
  [worker()]        --> How will you track which index in the request queue to remove next? 
  [dispatcher()]    --> How will you know where to insert the next request received into the request queue?
  [multiple funct]  --> How will you track the p_thread's that you create for workers? TODO
  How will you store the database of images? What data structure will you use? Example: database_entry_t database[100]; 
*/

// Global variables for tracking dispatcher and worker threads
pthread_t dispatcher_thread[MAX_THREADS]; // Array to store dispatcher thread IDs
pthread_t worker_thread[MAX_THREADS];     // Array to store worker thread IDs

// Mutex and condition variables for request queue
pthread_mutex_t req_queue_mutex = PTHREAD_MUTEX_INITIALIZER;  // Mutex for request queue
pthread_cond_t req_queue_not_empty = PTHREAD_COND_INITIALIZER; // Condition variable: request queue not empty
pthread_cond_t req_queue_not_full = PTHREAD_COND_INITIALIZER;  // Condition variable: request queue not full

// Request queue related variables
request_t req_entries[MAX_QUEUE_LEN];  // Request queue
int req_count = 0;                     // Number of requests currently in the queue
int req_insert_idx = 0;                // Index to insert the next request
int req_remove_idx = 0;                // Index to remove the next request

// Database variables
database_entry_t database[100];        // Array to store image database entries
int database_size = 0;                 // Number of images in the database


//TODO: Implement this function
/**********************************************
 * image_match
   - parameters:
      - input_image is the image data to compare
      - size is the size of the image data
   - returns:
       - database_entry_t that is the closest match to the input_image
************************************************/
database_entry_t image_match(char *input_image, int size)
{
    const char *closest_file = NULL;
    int closest_distance = INT_MAX;
    int closest_index = -1;

    for(int i = 0; i < database_size; i++)
    {
        const char *current_file = database[i].buffer;
        if (current_file == NULL) {
            continue;
        }
        int result = memcmp(input_image, current_file, size < database[i].file_size ? size : database[i].file_size);
        int distance = abs(result);

        if (distance < closest_distance) {
            closest_distance = distance;
            closest_file = current_file;
            closest_index = i;
        }
    }

    if (closest_index != -1) {
        return database[closest_index];
    } else {
        printf("No closest file found.\n");
        exit(EXIT_FAILURE);
    }
}

//TODO: Implement this function
/**********************************************
 * LogPrettyPrint
   - parameters:
      - to_write is expected to be an open file pointer, or it 
        can be NULL which means that the output is printed to the terminal
      - All other inputs are self explanatory or specified in the writeup
   - returns:
       - no return value
************************************************/
void LogPrettyPrint(FILE* to_write, int threadId, int requestNumber, char * file_name, int file_size){
    if (to_write != NULL) {
        fprintf(to_write, "Thread %d processed request %d: File %s of size %d bytes\n", threadId, requestNumber, file_name, file_size);
    } else {
        printf("Thread %d processed request %d: File %s of size %d bytes\n", threadId, requestNumber, file_name, file_size);
    }
}


/*
  TODO: Implement this function for Intermediate Submission
  * loadDatabase
    - parameters:
        - path is the path to the directory containing the images
    - returns:
        - no return value
    - Description:
        - Traverse the directory and load all the images into the database
          - Load the images from the directory into the database
          - You will need to read the images into memory
          - You will need to store the file name in the database_entry_t struct
          - You will need to store the file size in the database_entry_t struct
          - You will need to store the image data in the database_entry_t struct
          - You will need to increment the number of images in the database
*/
/***********/

void loadDatabase(char *path)
{
    struct dirent *entry;
    DIR *dir = opendir(path);
    if (dir == NULL)
    {
        perror("Failed to open directory");
        exit(EXIT_FAILURE);
    }

    while ((entry = readdir(dir)) != NULL)
    {
        if (entry->d_type == DT_REG) // Regular file
        {
            char filepath[BUFF_SIZE];
            snprintf(filepath, BUFF_SIZE, "%s/%s", path, entry->d_name);
            
            FILE *file = fopen(filepath, "rb");
            if (file == NULL)
            {
                perror("Failed to open file");
                continue;
            }

            fseek(file, 0, SEEK_END);
            long filesize = ftell(file);
            fseek(file, 0, SEEK_SET);

            char *buffer = (char *)malloc(filesize);
            if (buffer == NULL)
            {
                perror("Failed to allocate memory for file");
                fclose(file);
                continue;
            }

            fread(buffer, 1, filesize, file);
            fclose(file);

            // Store in database
            strcpy(database[database_size].file_name, entry->d_name);
            database[database_size].file_size = filesize;
            database[database_size].buffer = buffer;

            database_size++;
        }
    }
    closedir(dir);
}



void * dispatch(void *thread_id) 
{   
  while (1) 
  {
    size_t file_size = 0;

    /* TODO: Intermediate Submission
    *    Description:      Accept client connection
    *    Utility Function: int accept_connection(void)
    */
	int fd = accept_connection();
        if (fd < 0)
        {
            continue; // Ignore if connection fails
        }

    
    /* TODO: Intermediate Submission
    *    Description:      Get request from client
    *    Utility Function: char * get_request_server(int fd, size_t *filelength)
    */
        char *request = get_request_server(fd, &file_size);
        if (request == NULL)
        {
            close(fd);
            continue; // Ignore if request is invalid
        }


   /* TODO
    *    Description:      Add the request into the queue
        //(1) Copy the filename from get_request_server into allocated memory to put on request queue
        

        //(2) Request thread safe access to the request queue

        //(3) Check for a full queue... wait for an empty one which is signaled from req_queue_notfull

        //(4) Insert the request into the queue
        
        //(5) Update the queue index in a circular fashion (i.e. update on circular fashion which means when the queue is full we start from the beginning again) 

        //(6) Release the lock on the request queue and signal that the queue is not empty anymore
   */
        pthread_mutex_lock(&req_queue_mutex);

        // Wait until there is space in the queue
        while (req_count == MAX_QUEUE_LEN)
        {
            pthread_cond_wait(&req_queue_not_full, &req_queue_mutex);
        }

        // Add request details to queue
        req_entries[req_insert_idx].file_descriptor = fd;
        req_entries[req_insert_idx].file_size = file_size;
        req_entries[req_insert_idx].buffer = strdup(request);

        req_insert_idx = (req_insert_idx + 1) % MAX_QUEUE_LEN;
        req_count++;

        // Signal that the queue is not empty
        pthread_cond_signal(&req_queue_not_empty);
        pthread_mutex_unlock(&req_queue_mutex);

        free(request);
  }
    return NULL;
}

void * worker(void *thread_id) {

  // You may use them or not, depends on how you implement the function
  int num_request = 0;                                    //Integer for tracking each request for printing into the log file

  /* TODO : Intermediate Submission 
  *    Description:      Get the id as an input argument from arg, set it to ID
  */
  pthread_t tid = pthread_self();
  // remove when done
  printf("Worker TID: %lu\n", (unsigned long)tid);
    
  while (1) {
    /* TODO
    *    Description:      Get the request from the queue and do as follows
      //(1) Request thread safe access to the request queue by getting the req_queue_mutex lock
      //(2) While the request queue is empty conditionally wait for the request queue lock once the not empty signal is raised

      //(3) Now that you have the lock AND the queue is not empty, read from the request queue

      //(4) Update the request queue remove index in a circular fashion

      //(5) Fire the request queue not full signal to indicate the queue has a slot opened up and release the request queue lock  
      */
      pthread_mutex_lock(&req_queue_mutex);

      while (req_count == 0) {
        pthread_cond_wait(&req_queue_not_empty, &req_queue_mutex);
      }

      request_t request = req_entries[req_remove_idx];
      req_remove_idx = (req_remove_idx + 1) % MAX_QUEUE_LEN;
      req_count--;

      pthread_cond_signal(&req_queue_not_full);
      pthread_mutex_unlock(&req_queue_mutex);
        
      
    /* TODO
    *    Description:       Call image_match with the request buffer and file size
    *    store the result into a typeof database_entry_t
    *    send the file to the client using send_file_to_client(int fd, char * buffer, int size)              
    */
    database_entry_t match = image_match(request.buffer, request.file_size);
    send_file_to_client(request.file_descriptor, match.buffer, match.file_size);
    close(request.file_descriptor);

    /* TODO
    *    Description:       Call LogPrettyPrint() to print server log
    *    update the # of request (include the current one) this thread has already done, you may want to have a global array to store the number for each thread
    *    parameters passed in: refer to write up
    */
    num_request++;
    LogPrettyPrint(logfile, (unsigned long)tid, num_request, match.file_name, match.file_size);
  }
}

int main(int argc , char *argv[])
{
   if(argc != 6){
    printf("usage: %s port path num_dispatcher num_workers queue_length \n", argv[0]);
    return -1;
  }


  int port            = -1;
  char path[BUFF_SIZE] = "no path set\0";
  num_dispatcher      = -1;                               //global variable
  num_worker          = -1;                               //global variable
  queue_len           = -1;                               //global variable
 

  /* TODO: Intermediate Submission
  *    Description:      Get the input args --> (1) port (2) database path (3) num_dispatcher (4) num_workers  (5) queue_length
  */
  port = atoi(argv[1]);
  strcpy(path, argv[2]);
  num_dispatcher = atoi(argv[3]);
  num_worker = atoi(argv[4]);
  queue_len = atoi(argv[5]);
  

  /* TODO: Intermediate Submission
  *    Description:      Open log file
  *    Hint:             Use Global "File* logfile", use "server_log" as the name, what open flags do you want?
  */
  logfile = fopen("server_log", "w");
  if (logfile == NULL) {
    perror("Failed to open log file");
    exit(EXIT_FAILURE);
  }
  
 
  /* TODO: Intermediate Submission
  *    Description:      Start the server
  *    Utility Function: void init(int port); //look in utils.h 
  */
  init(port);


  /* TODO : Intermediate Submission
  *    Description:      Load the database
  *    Function: void loadDatabase(char *path); // prototype in server.h
  */
  loadDatabase(path);
 

  /* TODO: Intermediate Submission
  *    Description:      Create dispatcher and worker threads 
  *    Hints:            Use pthread_create, you will want to store pthread's globally
  *                      You will want to initialize some kind of global array to pass in thread ID's
  *                      How should you track this p_thread so you can terminate it later? [global]
  */
  for (int i = 0; i < num_dispatcher; i++) {
    int dThread = pthread_create(&dispatcher_thread[i], NULL, dispatch, NULL);
    if (dThread != 0) {
      printf("ERROR : Fail to create dispatcher thread %d.\n", i);
      exit(EXIT_FAILURE);
    }
  }

  // Creates our num_workers and stores in worker_thread[]
  for (int i = 0; i < num_worker; i++) {
    int wThread = pthread_create(&worker_thread[i], NULL, worker, NULL);
    if (wThread != 0) {
      printf("ERROR : Fail to create worker thread %d.\n", i);
      exit(EXIT_FAILURE);
    }
  }


  // Wait for each of the threads to complete their work
  // Threads (if created) will not exit (see while loop), but this keeps main from exiting
  int i;
  for(i = 0; i < num_dispatcher; i++){
    fprintf(stderr, "JOINING DISPATCHER %d \n",i);
    if((pthread_join(dispatcher_thread[i], NULL)) != 0){
      printf("ERROR : Fail to join dispatcher thread %d.\n", i);
    }
  }
  for(i = 0; i < num_worker; i++){
   // fprintf(stderr, "JOINING WORKER %d \n",i);
    if((pthread_join(worker_thread[i], NULL)) != 0){
      printf("ERROR : Fail to join worker thread %d.\n", i);
    }
  }
  fprintf(stderr, "SERVER DONE \n");  // will never be reached in SOLUTION
  fclose(logfile);//closing the log files

}
