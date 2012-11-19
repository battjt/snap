#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread_workqueue.h>

typedef int boolean;
struct snap {
	char * hash_dir;
	int verbosity;
	boolean try_to_link;
	int log_level;
	pthread_workqueue_t queue;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int count;
};

void msg(struct snap *env, int level, char *fmt, ...) {
	if (env->log_level > level) {
		va_list a;
		va_start(a, fmt);
		vprintf(fmt, a);
		va_end(a);
	}
}

char nibbleToHex(unsigned char b) {
	b &= 0xF;
	return b > 9 ? (b + 'A' - 10) : (b + '0');
}

void to_hex(char hex[SHA_DIGEST_LENGTH * 2],
		unsigned char hash[SHA_DIGEST_LENGTH]) {
	int i, j;
	for (i = 0, j = 0; i < SHA_DIGEST_LENGTH; i++, j += 2) {
		hex[j] = nibbleToHex(hash[i] >> 4);
		hex[j + 1] = nibbleToHex(hash[i]);
	}
}

void copyToRepo(struct snap *env, char * from, struct stat *from_stat,
		char *target) {
	msg(env, 10, "     copyToRepo  %s\n", from);
	int from_fd = open(from, O_RDONLY);
	void* map = mmap(NULL, from_stat->st_size, PROT_READ, MAP_SHARED, from_fd,
			0);
	if (map == MAP_FAILED ) {
		int e = errno;
		msg(env, 0, "error mapping file: %s\n  %d %s\n", from, e, strerror(e));
	}
	unsigned char hash[SHA_DIGEST_LENGTH];
	SHA1(map, from_stat->st_size, hash);
	munmap(map, from_stat->st_size);
	char hex[SHA_DIGEST_LENGTH * 2];
	to_hex(hex, hash);

	sprintf(target, "%s/%s", env->hash_dir, hex);
	msg(env, 10, "     target %s\n", target);

	struct stat stat_target;
	if (stat(target, &stat_target) && errno == ENOENT) {
		if (env->try_to_link) {
			if (link(from, target) == 0) {
				close(from_fd);
			}
		}
		// copy
		lseek(from_fd, 0, SEEK_SET);
		char buf[4096];
		int len;
		int target_fd = open(target, O_WRONLY | O_CREAT, 0x777);
		if (target_fd < 0) {
			int e = errno;
			msg(env, 0, "failed to write %s\n  %d %s\n", target, e,
					strerror(e));
		}
		while ((len = read(from_fd, buf, sizeof(buf))) > 0) {
			write(target_fd, buf, len);
		}
		close(target_fd);
	}
	close(from_fd);
}
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

void incCount(struct snap* env, int c) {
	pthread_mutex_lock(env->mutex, NULL );
	env->count += c;
	if (env->count == 0) {
		pthread_cond_signal(env->cond);
	}
	pthread_mutex_unlock(env->mutex);
}

void snapdir(void *data) {
	struct snapdir *d = (struct snapdir *) data;
	incCount(d, 1);
	// recurse
	DIR* dir = opendir(d->from);
	struct dirent* de;
	char to_new[strlen(d->to) + FILENAME_MAX + 2];
	char from_new[strlen(d->from) + FILENAME_MAX + 2];
	ensure_dir(d->env, d->to);
	while ((de = readdir(dir)) != NULL ) {
		if (strncmp(".", de->d_name, 2) && strncmp("..", de->d_name, 3)) {
			sprintf(to_new, "%s/%s", d->to, de->d_name);
			sprintf(from_new, "%s/%s", d->from, de->d_name);
			snap(d->env, from_new, to_new);
		}
	}
	incCount(d->env, -1);
}

void snap(struct snap *env, char* from, char *to) {
	msg(env, 5, "snap from %s to %s\n", from, to);
	struct stat statb;
	stat(from, &statb);
	if (S_IFDIR & statb.st_mode) {
		msg(env, 8, "     dir  %s\n", from);
		struct snapdir *d = malloc(sizeof(struct snapdir));
		*d = (struct snapdir ) { env, from, to };
		pthread_workqueue_additem_np(env->queue, snapdir, d, NULL, NULL );
	} else if (S_IFREG & statb.st_mode) {
		char copy[strlen(env->hash_dir) + FILENAME_MAX + 2];
		copyToRepo(env, from, &statb, copy);
		msg(env, 3, "link: %s -> %s\n", copy, to);
		link(copy, to);
	} else {
		msg(env, 0, "Unhandled file type: %s\n", from);
	}
}

int main(int argc, char *argv[]) {
	if (argc < 4) {
		fprintf(stderr, "Usage: %s backup_dir label files*\n", argv[0]);
		exit(-1);
	}
	struct snap env;
	env.log_level = 100;
	env.hash_dir = alloca(strlen(argv[1]) + strlen("/hash"));
	strcat(strcpy(env.hash_dir, argv[1]), "/hash");
	env.try_to_link = 1;
	pthread_mutex_init(&env.mutex, NULL );
	pthread_cond_init(&env.cond, &env.mutex);

	ensure_dir(&env, argv[1]);
	ensure_dir(&env, env.hash_dir);

	pthread_workqueue_create_np(&env.queue, NULL );

	char to[strlen(argv[1]) + strlen(argv[2]) + 2];
	strcat(strcat(strcpy(to, argv[1]), "/"), argv[2]);
	int i;
	incCount(&env, 1);
	for (i = 3; i < argc; i++) {
		snap(&env, argv[i], to);
	}
	incCount(&env, -1);
	// wait for all done
	pthread_mutex_lock(&env.mutex);
	pthread_cond_wait(&env.mutex, &env.cond);
	pthread_mutex_unlock(&env.mutex);
	return 0;
}
