/* Ordinary POSIX filter: no DACP or Arrow dependency. */
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

int main(int argc, char **argv) {
    unsigned char buffer[4096];
    ssize_t count;
    while ((count = read(STDIN_FILENO, buffer, sizeof buffer)) > 0) {
        for (ssize_t i = 0; i < count; ++i) buffer[i] = (unsigned char)toupper(buffer[i]);
        for (ssize_t offset = 0; offset < count;) {
            ssize_t written = write(STDOUT_FILENO, buffer + offset, count - offset);
            if (written < 0) { if (errno == EINTR) continue; return 2; }
            offset += written;
        }
        if (argc == 2 && strcmp(argv[1], "--fail") == 0) {
            fprintf(stderr, "deliberate native failure\n");
            return 7;
        }
        if (argc == 2 && strcmp(argv[1], "--slow") == 0) sleep(15);
    }
    return count < 0 ? 1 : 0;
}
