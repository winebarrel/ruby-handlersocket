#ifndef __HANDLERSOCKET_H__
#define __HANDLERSOCKET_H__

#include "ruby.h"

#ifndef RSTRING_PTR
#define RSTRING_PTR(s) (RSTRING(s)->ptr)
#endif
#ifndef RSTRING_LEN
#define RSTRING_LEN(s) (RSTRING(s)->len)
#endif

#ifndef RARRAY_PTR
#define RARRAY_PTR(a) (RARRAY(a)->ptr)
#endif
#ifndef RARRAY_LEN
#define RARRAY_LEN(a) (RARRAY(a)->len)
#endif

#define __F(f) (reinterpret_cast<VALUE (*)(...)>(f))

#define Check_TcpCli(p) do { \
  if (!(p)->tcpcli) { \
    rb_raise(rb_eRuntimeError, "Not initialized"); \
  } \
} while(0)

extern "C" {
  void Init_handlersocket();
}

#endif // __HANDLERSOCKET_H__
