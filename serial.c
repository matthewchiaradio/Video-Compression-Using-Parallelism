#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <time.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define NUM_THREADS 8 // Number of threads
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t total = PTHREAD_MUTEX_INITIALIZER;
int counter = 1;

struct ThreadData {
    char *dir_path; // directory path
    char *file_name; // name of file ex: 0001.ppm
    FILE *output_file; // file to write
    int *total_in; // total in
    int *total_out; // total out
};

int cmp(const void *a, const void *b) {
    return strcmp(*(char **) a, *(char **) b);
}

// Thread function, 
void *compress_file(void *args) {
    struct ThreadData *data = (struct ThreadData *)args;

    // create full path of file
    char *full_path = malloc((strlen(data->dir_path) + strlen(data->file_name) + 2) * sizeof(char));
    strcpy(full_path, data->dir_path);
    strcat(full_path, "/");
    strcat(full_path, data->file_name);

    unsigned char buffer_in[BUFFER_SIZE];
    unsigned char buffer_out[BUFFER_SIZE];

    FILE *f_in = fopen(full_path, "r");
    assert(f_in != NULL);
    int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
    fclose(f_in);

    pthread_mutex_lock(&total);
    *(data->total_in) += nbytes;
    pthread_mutex_unlock(&total);

    z_stream strm;
    int ret = deflateInit(&strm, 9);
    assert(ret == Z_OK);
    strm.avail_in = nbytes;
    strm.next_in = buffer_in;
    strm.avail_out = BUFFER_SIZE;
    strm.next_out = buffer_out;

    ret = deflate(&strm, Z_FINISH);
    assert(ret == Z_STREAM_END);

    int nbytes_zipped = BUFFER_SIZE - strm.avail_out;

    char filenum[4];
	strncpy(filenum, data->file_name, 4); // get number
	int num = atoi(filenum); // Convert number to int
    //printf("%s %d\n", data->file_name, num);

    while(num != counter); // spin lock for ordering

    pthread_mutex_lock(&mutex); // only one thread can write to file at 1 time
    fwrite(&nbytes_zipped, sizeof(int), 1, data->output_file);
    fwrite(buffer_out, sizeof(unsigned char), nbytes_zipped, data->output_file);
    *(data->total_out) += nbytes_zipped; // in a lock so seperate mutex is not needed
    counter++; // next thread can execute writing to file
    pthread_mutex_unlock(&mutex);

    free(full_path);
    deflateEnd(&strm);

    return NULL;
}

int main(int argc, char **argv) {
    // time computation header
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	// end of time computation header

	// do not modify the main function before this point!

    assert(argc == 2);

    DIR *d;
    struct dirent *dir;
    char **files = NULL;
    int nfiles = 0;

    d = opendir(argv[1]);
    if (d == NULL) {
        printf("An error has occurred\n");
        return 0;
    }

    // create sorted list of PPM files
    while ((dir = readdir(d)) != NULL) {
        files = realloc(files, (nfiles + 1) * sizeof(char *));
        assert(files != NULL);

        int len = strlen(dir->d_name);
        if (dir->d_name[len - 4] == '.' && dir->d_name[len - 3] == 'p' && dir->d_name[len - 2] == 'p' && dir->d_name[len - 1] == 'm') {
            files[nfiles] = strdup(dir->d_name);
            assert(files[nfiles] != NULL);
            nfiles++;
        }
    }
    closedir(d);
    qsort(files, nfiles, sizeof(char *), cmp);

    FILE *f_out = fopen("video.vzip", "w"); // Open file to write, stays open while threads execute
    assert(f_out != NULL);

    int total_in = 0, total_out = 0;
    pthread_t threads[NUM_THREADS]; // create threads
    struct ThreadData thread_data[NUM_THREADS];

    for (int i = 0; i < nfiles; i++) {
        if (i >= NUM_THREADS) { // Max threads to be ran at once
            pthread_join(threads[i % NUM_THREADS], NULL);
        }

        thread_data[i % NUM_THREADS].dir_path = argv[1];
        thread_data[i % NUM_THREADS].file_name = files[i];
        thread_data[i % NUM_THREADS].output_file = f_out;
        thread_data[i % NUM_THREADS].total_in = &total_in; // point all threads to same value
        thread_data[i % NUM_THREADS].total_out = &total_out; // point all threads to same value

        pthread_create(&threads[i % NUM_THREADS], NULL, compress_file, (void *)&thread_data[i % NUM_THREADS]);
    }

    for (int i = 0; i < nfiles % NUM_THREADS; i++) {
        pthread_join(threads[i], NULL); // join any remanding threads
    }

    fclose(f_out);

    printf("Compression rate: %.2lf%%\n", 100.0 * (total_in - total_out) / total_in);

    for (int i = 0; i < nfiles; i++)
        free(files[i]);
    free(files);

    // do not modify the main function after this point!

	// time computation footer
    clock_gettime(CLOCK_MONOTONIC, &end);
    printf("Time: %.2f seconds\n", ((double)end.tv_sec + 1.0e-9 * end.tv_nsec) - ((double)start.tv_sec + 1.0e-9 * start.tv_nsec));
    // end of time computation footer

    return 0;
}
