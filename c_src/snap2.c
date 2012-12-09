#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include <pthread_workqueue.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

typedef int boolean;

/*
 * Structure to represent a single snapshot.  This stuff could be global, but due to my upbringing I'm unable to use globals.
 */
struct snap {
  char * hash_dir;
  char *snap_dir;
  int verbosity;
  boolean try_to_link;
  int log_level;
  pthread_workqueue_t queue;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int outstandingTasks;
};

/*
 * Wrapper for fprintf(stderr... with a level for logging
 */
void msg(struct snap *env, int level, char *fmt, ...) {
  if (env->log_level >= level) {
    va_list a;
    va_start(a, fmt);
    vfprintf(stderr, fmt, a);
    va_end(a);
  }
}

char nibbleToHex(unsigned char b) {
  b &= 0xF;
  return b > 9 ? (b + 'A' - 10) : (b + '0');
}

/*
 * Convert a char* of data to a char* of hex digits.  The length of hex will be 2*x+1 (for NULL termination).
 */
void to_hex(int length, unsigned char hash[], char hex[]) {
  int i, j;
  for (i = 0, j = 0; i < length; i++) {
    hex[j++] = nibbleToHex(hash[i] >> 4);
    hex[j++] = nibbleToHex(hash[i]);
  }
  // always NULL terminate your strings, Java programmers.
  hex[length * 2] = '\0';
}

/*
 *  Copy (link if we can) the file to the repo returning the hash filename.
 *  If it's already there, just return the name.
 */
void copyToRepo(struct snap *env, char * from, struct stat *from_stat,
    char *target) {
  msg(env, 1, "%d\t%s\n", from_stat->st_size, from);

  // get sha1 of file contents to use as a name in repo
  int from_fd = open(from, O_RDONLY);
  if (from_fd < 1) {
    int e = errno;
    msg(env, 0, "Failed to open %s.  %d %s\n", from, e, strerror(e));
    return;
  }
  void* map = mmap(NULL, from_stat->st_size, PROT_READ, MAP_SHARED, from_fd, 0);
  if (map == MAP_FAILED ) {
    int e = errno;
    msg(env, 0, "error mapping file: %s\n  %d %s\n", from, e, strerror(e));
    close(from_fd);
    return;
  }
  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1(map, from_stat->st_size, hash);
  munmap(map, from_stat->st_size);

  // convert to char *
  char hex[SHA_DIGEST_LENGTH * 2 + 1];
  to_hex(SHA_DIGEST_LENGTH, hash, hex);

  // convert to filename
  strcat(strcat(strcpy(target, env->hash_dir), "/"), hex);
  msg(env, 10, "     target %s\n", target);

  // if it's not there, link it or copy on failure
  struct stat stat_target;
  if (stat(target, &stat_target) && errno == ENOENT) {
    if (env->try_to_link) {
      if (link(from, target) == 0) {
        close(from_fd);
        return;
      }
      msg(env, 1, "Failed to link, copying instead %s to %s\n", from, target);
    }
    // copy
    //int from_fd = open(from, O_RDONLY);
    lseek(from_fd, 0, SEEK_SET);
    if (from_fd < 1) {
      int e = errno;
      msg(env, 0, "Failed to open %s.  %d %s\n", from, e, strerror(e));
    }
    int target_fd = open(target, O_WRONLY | O_CREAT, 0x777);
    if (target_fd < 0) {
      int e = errno;
      msg(env, 0, "failed to open for write %s %d %s\n", target, e,
          strerror(e));
    }
    char buf[4096];
    int len;
    while ((len = read(from_fd, buf, sizeof(buf))) > 0) {
      if (write(target_fd, buf, len) < 0) {
        int e = errno;
        msg(env, 0, "failed to write %s: %d: %s\n", target, e,
            strerror(e));
      }
    }
    close(target_fd);
    close(from_fd);
  } else {
    if (utime(target, NULL )) {
      int e = errno;
      msg(env, 0, "failed to update time on %s: %d: %s\n", target, e,
          strerror(e));
    }
  }
  close(from_fd);
}

/* allocate directory if it's not there */
void ensure_dir(struct snap* env, char* dir) {
  struct stat b;
  if (stat(dir, &b) && errno == ENOENT) {
    msg(env, 10, "mkdir %s\n", dir);
    mkdir(dir, 0777);
  }
}

struct snapdir {
  struct snap *env;
  char *from;
  char *to;
};

void snap(struct snap *env, char* from, char *to);

void incrementOutstandingTasks(struct snap* env, int c) {
  msg(env, 5, "incCount: by %d to %d lock\n", c, env->outstandingTasks);
  pthread_mutex_lock(&env->mutex);
  env->outstandingTasks += c;
  msg(env, 5, "        : by %d to %d done\n", c, env->outstandingTasks);
  if (env->outstandingTasks == 0) {
    pthread_cond_signal(&env->cond);
  }
  pthread_mutex_unlock(&env->mutex);
}

void snapDir_task_fn(void *data) {
  struct snapdir *d = (struct snapdir *) data;
  // recurse
  msg(d->env, 3, "w: %s -> %s", d->from, d->to);
  DIR* dir = opendir(d->from);
  if (dir == NULL ) {
    int e = errno;
    msg(d->env, 0, "Failed to open %s.\n  %d %s", d->from, e, strerror(e));
  } else {
    struct dirent* de;
    char to_new[strlen(d->to) + FILENAME_MAX + 2];
    char from_new[strlen(d->from) + FILENAME_MAX + 2];
    ensure_dir(d->env, d->to);
    while ((de = readdir(dir)) != NULL ) {
      if (strncmp(".", de->d_name, 2) && strncmp("..", de->d_name, 3)) {
        strcat(strcat(strcpy(to_new, d->to), "/"), de->d_name);
        strcat(strcat(strcpy(from_new, d->from), "/"), de->d_name);
        snap(d->env, from_new, to_new);
      }
    }
    closedir(dir);
  }

  incrementOutstandingTasks(d->env, -1);
  free(d->from);
  free(d->to);
  free(d);
}
/*
 * Meat of project.  Copy from into the hash repo and make a link from the label directory.
 */
void snap(struct snap *env, char* from, char *to) {
  msg(env, 5, "snap from %s to %s\n", from, to);
  struct stat statb;
  stat(from, &statb);
  if (S_ISDIR(statb.st_mode)) {
    if (!strcmp(from, env->snap_dir)) {
      // avoid recursion on "snap ~/.snap $(date) ~/"
      msg(env, 1, "not taking snap of %s because it is snap dir\n", from);
      return;
    }
    // It's a directory, so put it on the work queue for a thread to do it.
    incrementOutstandingTasks(env, 1);
    msg(env, 3, "     dir  %s\n", from);
    struct snapdir *data = malloc(sizeof(struct snapdir));
    char *copy(char *s) {
      return strcpy(malloc(strlen(s) + 1), s);
    }
    *data = (struct snapdir ) { env, copy(from), copy(to) };
    if (pthread_workqueue_additem_np(env->queue, snapDir_task_fn, data, NULL,
        NULL )) {
      int e = errno;
      msg(env, 0, "Failed to submit task %d %s\n", e, strerror(e));
    }
  } else if (S_ISREG(statb.st_mode)) {
    char copy[strlen(env->hash_dir) + FILENAME_MAX + 2];
    copyToRepo(env, from, &statb, copy);
    msg(env, 5, "link: %s -> %s\n", copy, to);
    if (link(copy, to)) {
      int e = errno;
      msg(env, 0, "failed to link %s to %s: %d: %s\n", copy, to, e,
          strerror(e));
    }
  } else {
    msg(env, 0, "Unhandled file type: %s\n", from);
  }
}

int main(int argc, char *argv[]) {
  if (argc < 4) {
    fprintf(stderr,
        "Usage: %s log_level backup_dir label files*\n  log_level 0:errors 1:files and errors >: increasing levels of debugging\n",
        argv[0]);
    exit(-1);
  }
  struct snap env;
  env.log_level = atoi(argv[1]);
  env.snap_dir = realpath(argv[2], alloca(4096));
  env.hash_dir = alloca(strlen(argv[2]) + strlen("/hash")+1);
  strcat(strcpy(env.hash_dir, argv[2]), "/hash");
  env.try_to_link = 1;
  env.outstandingTasks = 0;
  msg(&env, 20, "starting:\n  argv[2]=%s\n  snap_dir=%s\n  hash_dir=%s\n",
      argv[2], env.snap_dir,
      env.hash_dir);
  if (pthread_mutex_init(&env.mutex, NULL )) {
    int e = errno;
    msg(&env, 0, "error pthread_mutex_init: %s\n", strerror(e));
  }
  if (pthread_cond_init(&env.cond, NULL )) {
    int e = errno;
    msg(&env, 0, "error pthread_cond_init: %s\n", strerror(e));
  }

  ensure_dir(&env, argv[2]);
  ensure_dir(&env, env.hash_dir);

  pthread_workqueue_create_np(&env.queue, NULL );

  char to[strlen(argv[2]) + strlen(argv[3]) + 2];
  strcat(strcat(strcpy(to, argv[2]), "/"), argv[3]);
  int i;
  for (i = 4; i < argc; i++) {
    snap(&env, realpath(argv[i], alloca(4096)), to);
  }
  if (env.outstandingTasks > 0) {
    // wait for all done
    pthread_mutex_lock(&env.mutex);
    pthread_cond_wait(&env.cond, &env.mutex);
    pthread_mutex_unlock(&env.mutex);
  }
  return 0;
}
