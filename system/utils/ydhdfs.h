#ifndef YDHDFS_H
#define YDHDFS_H

#define YARN

#ifdef YARN
#include "ydhdfs2.h"
#else
#include "ydhdfs1.h"
#endif

#endif
