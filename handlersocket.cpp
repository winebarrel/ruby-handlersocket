#include <libhsclient/hstcpcli.cpp>
#include "handlersocket.h"

namespace {

void ary2vec(VALUE ary, std::vector<dena::string_ref>& vec) {
  int len = RARRAY_LEN(ary);

  for (int i = 0; i < len; i++) {
    VALUE entry = rb_ary_entry(ary, i);

    if (NIL_P(entry)) {
      vec.push_back(dena::string_ref());
    } else {
      entry = rb_check_convert_type(entry, T_STRING, "String", "to_s");
      char *s = StringValuePtr(entry);
      vec.push_back(dena::string_ref(s, strlen(s)));
    }
  }
}

struct HandlerSocket {
  dena::hstcpcli_i *tcpcli;

  static void free(HandlerSocket *p) {
    if (p->tcpcli) {
      delete p->tcpcli;
    }

    delete p;
  }

  static VALUE alloc(VALUE klass) {
    HandlerSocket *p;

    p = new HandlerSocket;
    p->tcpcli = NULL;

    return Data_Wrap_Struct(klass, 0, &free, p);
  }

  static VALUE initialize(int argc, VALUE *argv, VALUE self) {
    HandlerSocket *p;
    VALUE host, port, timeout, listen_backlog, verbose_level;

    rb_scan_args(argc, argv, "05", &host, &port, &timeout, &listen_backlog, &verbose_level);

    dena::config conf;

    if (NIL_P(host)) {
      conf["host"] = "localhost";
    } else {
      Check_Type(host, T_STRING);
      conf["host"] = RSTRING_PTR(host);
    }

    if (NIL_P(port)) {
      conf["port"] = "9998";
    } else {
      Check_Type(port, T_FIXNUM);
      port = rb_check_convert_type(port, T_STRING, "String", "to_s");
      conf["port"] = StringValuePtr(port);
    }

    if (NIL_P(timeout)) {
      conf["timeout"] = "600";
    } else {
      Check_Type(timeout, T_FIXNUM);
      timeout = rb_check_convert_type(timeout, T_STRING, "String", "to_s");
      conf["timeout"] = StringValuePtr(timeout);
    }

    if (NIL_P(listen_backlog)) {
      conf["listen_backlog"] = "256";
    } else {
      Check_Type(listen_backlog, T_FIXNUM);
      listen_backlog = rb_check_convert_type(listen_backlog, T_STRING, "String", "to_s");
      conf["listen_backlog"] = StringValuePtr(listen_backlog);
    }

    if (!NIL_P(verbose_level)) {
      dena::verbose_level = NUM2INT(verbose_level);
    }

    dena::socket_args sargs;
    sargs.set(conf);
    dena::hstcpcli_ptr tcpcli_ptr = dena::hstcpcli_i::create(sargs);

    Data_Get_Struct(self, HandlerSocket, p);
    p->tcpcli = tcpcli_ptr.get();

    tcpcli_ptr.release();

    return Qnil;
  }

  static VALUE close(VALUE self) {
    HandlerSocket *p;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);

    p->tcpcli->close();

    return Qnil;
  }

  static VALUE reconnect(VALUE self) {
    HandlerSocket *p;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);

    int retval = p->tcpcli->reconnect();

    return INT2FIX(retval);
  }

  static VALUE stable_point(VALUE self) {
    HandlerSocket *p;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);

    bool retval = p->tcpcli->stable_point();

    return retval ? Qtrue : Qfalse;
  }

  static VALUE get_error_code(VALUE self) {
    HandlerSocket *p;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);

    int retval = p->tcpcli->get_error_code();

    return INT2FIX(retval);
  }

  static VALUE get_error(VALUE self) {
    HandlerSocket *p;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);

    std::string s = p->tcpcli->get_error();

    return rb_str_new(s.data(), s.size());
  }

  static VALUE open_index(VALUE self, VALUE id, VALUE db, VALUE table, VALUE index, VALUE fields) {
    HandlerSocket *p;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);
    Check_Type(id, T_FIXNUM);
    Check_Type(db, T_STRING);
    Check_Type(table, T_STRING);
    Check_Type(index, T_STRING);
    Check_Type(fields, T_STRING);

    do {
      p->tcpcli->request_buf_open_index(
        FIX2INT(id), RSTRING_PTR(db), RSTRING_PTR(table), RSTRING_PTR(index),RSTRING_PTR(fields));

      if (p->tcpcli->request_send() != 0) {
        break;
      }

      size_t nflds = 0;
      p->tcpcli->response_recv(nflds);

      int e = p->tcpcli->get_error_code();

      if (e >= 0) {
        p->tcpcli->response_buf_remove();
      }
    } while (0);

    int retval = p->tcpcli->get_error_code();

    return INT2FIX(retval);
  }

  static VALUE execute_single(int argc, VALUE *argv, VALUE self) {
    VALUE id, op, keys, limit, skip, modo, modvals;
    char *modop = NULL;

    rb_scan_args(argc, argv, "34", &id, &op, &keys, &limit, &skip, &modo, &modvals);

    if (NIL_P(limit)) {
      limit = INT2FIX(0);
    }

    if (NIL_P(skip)) {
      skip = INT2FIX(0);
    }

    if (!NIL_P(modo)) {
      Check_Type(modo, T_STRING);
      Check_Type(modvals, T_ARRAY);
      modop = RSTRING_PTR(modo);
    }

    return execute_internal(self, id, op, keys, limit, skip, modop, modvals);
  }

  static VALUE execute_multi(VALUE self, VALUE args) {
    return execute_multi_internal(self, args);
  }

  static VALUE execute_update(VALUE self, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip, VALUE modvals) {
    return execute_internal(self, id, op, keys, limit, skip, "U", modvals);
  }

  static VALUE execute_delete(VALUE self, VALUE id, VALUE op, VALUE keys, VALUE limit, VALUE skip) {
    return execute_internal(self, id, op, keys, limit, skip, "D", rb_ary_new());
  }

  static VALUE execute_insert(VALUE self, VALUE id, VALUE fvals) {
    VALUE op = rb_str_new("+", 1);
    char *modop = NULL;
    return execute_internal(self, id, op, fvals, INT2FIX(0), INT2FIX(0), modop, Qnil);
  }

  static void init() {
    VALUE rb_cHandlerSocket = rb_define_class("HandlerSocket", rb_cObject);
    rb_define_alloc_func(rb_cHandlerSocket, &alloc);

    rb_define_method(rb_cHandlerSocket, "initialize", __F(&initialize), -1);
    rb_define_method(rb_cHandlerSocket, "close", __F(&close), 0);
    rb_define_method(rb_cHandlerSocket, "reconnect", __F(&reconnect), 0);
    rb_define_method(rb_cHandlerSocket, "stable_point", __F(&stable_point), 0);
    rb_define_method(rb_cHandlerSocket, "get_error_code", __F(&get_error_code), 0);
    rb_define_method(rb_cHandlerSocket, "get_error", __F(&get_error), 0);
    rb_define_method(rb_cHandlerSocket, "open_index", __F(&open_index), 5);
    rb_define_method(rb_cHandlerSocket, "execute_single", __F(&execute_single), -1);
    rb_define_method(rb_cHandlerSocket, "execute_find", __F(&execute_single), -1);
    rb_define_method(rb_cHandlerSocket, "execute_multi", __F(&execute_multi), 1);
    rb_define_method(rb_cHandlerSocket, "execute_update", __F(&execute_update), 6);
    rb_define_method(rb_cHandlerSocket, "execute_delete", __F(&execute_delete), 5);
    rb_define_method(rb_cHandlerSocket, "execute_insert", __F(&execute_insert), 2);
  }

private:
  static VALUE execute_internal(VALUE self, VALUE v_id, VALUE v_op, VALUE v_keys, VALUE v_limit, VALUE v_skip, char *modop, VALUE v_modvals) {
    HandlerSocket *p;
    VALUE retval = Qnil;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);
    Check_Type(v_id, T_FIXNUM);
    Check_Type(v_op, T_STRING);
    Check_Type(v_keys, T_ARRAY);
    Check_Type(v_limit, T_FIXNUM);
    Check_Type(v_skip, T_FIXNUM);

    do {
      dena::hstcpcli_i *tcpcli = p->tcpcli;
      int id = FIX2INT(v_id);
      dena::string_ref op = dena::string_ref(RSTRING_PTR(v_op), RSTRING_LEN(v_op));
      std::vector<dena::string_ref> keyarr, mvarr;
      ary2vec(v_keys, keyarr);
      dena::string_ref modop_ref;

      if (modop) {
        modop_ref = dena::string_ref(modop, strlen(modop));
        ary2vec(v_modvals, mvarr);
      }

      int limit = FIX2INT(v_limit);
      int skip = FIX2INT(v_skip);

      tcpcli->request_buf_exec_generic(
        id, op, &keyarr[0], keyarr.size(), limit, skip, modop_ref, &mvarr[0], mvarr.size());

      if (tcpcli->request_send() != 0) {
        break;
      }

      size_t nflds = 0;
      tcpcli->response_recv(nflds);

      int e = tcpcli->get_error_code();
      retval = rb_ary_new();
      rb_ary_push(retval, INT2FIX(e));

      if (e != 0) {
        std::string s = tcpcli->get_error();
        rb_ary_push(retval, rb_str_new(s.data(), s.size()));
      } else {
        const dena::string_ref *row = 0;

        while ((row = tcpcli->get_next_row()) != 0) {
          for (size_t i = 0; i < nflds; i++) {
            const dena::string_ref& v = row[i];

            if (v.begin() != 0) {
              VALUE s = rb_str_new(v.begin(), v.size());
              rb_ary_push(retval, s);
            } else {
              rb_ary_push(retval, Qnil);
            }
          }
        }
      }

      if (e >= 0) {
        tcpcli->response_buf_remove();
      }
     } while(0);

    return retval;
  }

  static VALUE execute_multi_internal(VALUE self, VALUE args) {
    HandlerSocket *p;
    VALUE rvs = Qnil;

    Data_Get_Struct(self, HandlerSocket, p);
    Check_TcpCli(p);
    Check_Type(args, T_ARRAY);

    dena::hstcpcli_i *tcpcli = p->tcpcli;
    size_t num_args = RARRAY_LEN(args);

    for (size_t args_index = 0; args_index < num_args; args_index++) {
      VALUE v_arg = rb_ary_entry(args, args_index);
      Check_Type(v_arg, T_ARRAY);

      VALUE v_id = rb_ary_entry(v_arg, 0);
      VALUE v_op = rb_ary_entry(v_arg, 1);
      VALUE v_keys = rb_ary_entry(v_arg, 2);
      VALUE v_limit = rb_ary_entry(v_arg, 3);
      VALUE v_skip = rb_ary_entry(v_arg, 4);
      VALUE v_modo = rb_ary_entry(v_arg, 5);
      VALUE v_modvals = rb_ary_entry(v_arg, 6);

      Check_Type(v_id, T_FIXNUM);
      Check_Type(v_op, T_STRING);
      Check_Type(v_keys, T_ARRAY);
      Check_Type(v_limit, T_FIXNUM);
      Check_Type(v_skip, T_FIXNUM);

      if (!NIL_P(v_modo)) {
        Check_Type(v_modo, T_STRING);
        Check_Type(v_modvals, T_ARRAY);
      }
    }

    for (size_t args_index = 0; args_index < num_args; args_index++) {
      VALUE v_arg = rb_ary_entry(args, args_index);
      VALUE v_id = rb_ary_entry(v_arg, 0);
      VALUE v_op = rb_ary_entry(v_arg, 1);
      VALUE v_keys = rb_ary_entry(v_arg, 2);
      VALUE v_limit = rb_ary_entry(v_arg, 3);
      VALUE v_skip = rb_ary_entry(v_arg, 4);
      VALUE v_modo = rb_ary_entry(v_arg, 5);
      VALUE v_modvals = rb_ary_entry(v_arg, 6);

      int id = FIX2INT(v_id);
      dena::string_ref op = dena::string_ref(RSTRING_PTR(v_op), RSTRING_LEN(v_op));
      std::vector<dena::string_ref> keyarr, mvarr;
      ary2vec(v_keys, keyarr);
      dena::string_ref modop_ref;

      if (!NIL_P(v_modo)) {
        modop_ref = dena::string_ref(RSTRING_PTR(v_modo), RSTRING_LEN(v_modo));
        ary2vec(v_modvals, mvarr);
      }

      int limit = FIX2INT(v_limit);
      int skip = FIX2INT(v_skip);

      tcpcli->request_buf_exec_generic(
        id, op, &keyarr[0], keyarr.size(), limit, skip, modop_ref, &mvarr[0], mvarr.size());
    }

    rvs = rb_ary_new();

    if (tcpcli->request_send() < 0) {
      VALUE retval = rb_ary_new();
      rb_ary_push(rvs, retval);
      rb_ary_push(retval, INT2FIX(tcpcli->get_error_code()));
      const std::string& s = tcpcli->get_error();
      rb_ary_push(retval, rb_str_new(s.data(), s.size()));
      return rvs;
    }

    for (size_t args_index = 0; args_index < num_args; args_index++) {
      VALUE retval = rb_ary_new();
      rb_ary_push(rvs, retval);

      size_t nflds = 0;
      int e = tcpcli->response_recv(nflds);
      rb_ary_push(retval, INT2FIX(e));

      if (e != 0) {
        const std::string& s = tcpcli->get_error();
        rb_ary_push(retval, rb_str_new(s.data(), s.size()));
      } else {
        const dena::string_ref *row = 0;

        while ((row = tcpcli->get_next_row()) != 0) {
          for (size_t i = 0; i < nflds; i++) {
            const dena::string_ref& v = row[i];

            if (v.begin() != 0) {
              VALUE s = rb_str_new(v.begin(), v.size());
              rb_ary_push(retval, s);
            } else {
              rb_ary_push(retval, Qnil);
            }
          }
        }
      }

      if (e >= 0) {
        tcpcli->response_buf_remove();
      }

      if (e < 0) {
        return rvs;
      }
    }

    return rvs;
  }

};

} // namespace

void Init_handlersocket() {
  HandlerSocket::init();
}
