#include "ruby.h"

typedef struct {
    unsigned char *ptr;
    unsigned char *endp;
} data_t;

static VALUE s_lcb(VALUE klass, VALUE val)
{
    unsigned long long n;
    unsigned char buf[9];

    if (val == Qnil)
        return rb_str_new("\xfb", 1);
    n = NUM2ULL(val);
    if (n < 251) {
        buf[0] = n;
        return rb_str_new(buf, 1);
    }
    if (n < 65536) {
        buf[0] = '\xfc';
        buf[1] = n % 256;
        buf[2] = n / 256;
        return rb_str_new(buf, 3);
    }
    if (n < 16777216) {
        buf[0] = '\xfd';
        buf[1] = n % 256;
        n /= 256;
        buf[2] = n % 256;
        buf[3] = n / 256;
        return rb_str_new(buf, 4);
    }
    buf[0] = '\xfe';
    buf[1] = n % 256;
    n /= 256;
    buf[2] = n % 256;
    n /= 256;
    buf[3] = n % 256;
    n /= 256;
    buf[4] = n % 256;
    n /= 256;
    buf[5] = n % 256;
    n /= 256;
    buf[6] = n % 256;
    n /= 256;
    buf[7] = n % 256;
    buf[8] = n / 256;
    return rb_str_new(buf, 9);
}

static VALUE s_lcs(VALUE klass, VALUE val)
{
    VALUE ret = s_lcb(klass, ULONG2NUM(RSTRING_LEN(val)));
    return rb_str_cat(ret, RSTRING_PTR(val), RSTRING_LEN(val));
}

static VALUE allocate(VALUE klass)
{
    data_t *data;

    data = xmalloc(sizeof *data);
    data->ptr = NULL;
    data->endp = NULL;
    return Data_Wrap_Struct(klass, 0, xfree, data);
}

static VALUE initialize(VALUE obj, VALUE buf)
{
    data_t *data;

    Data_Get_Struct(obj, data_t, data);
    rb_ivar_set(obj, rb_intern("buf"), buf);
    data->ptr = RSTRING_PTR(buf);
    data->endp = data->ptr + RSTRING_LEN(buf);
}

#define NIL_VALUE 0xFFFFFFFFFFFFFFFF

static unsigned long long _lcb(data_t *data)
{
    unsigned char v;
    unsigned long long n;

    if (data->ptr >= data->endp)
        return NIL_VALUE;

    v = *data->ptr++;
    switch (v) {
    case 0xfb:
        return NIL_VALUE;
    case 0xfc:
        n = *data->ptr++;
        n |= ((unsigned int)*data->ptr++) << 8;
        return n;
    case 0xfd:
        n = *data->ptr++;
        n |= ((unsigned int)*data->ptr++) << 8;
        n |= ((unsigned int)*data->ptr++) << 16;
        return n;
    case 0xfe:
        n = *data->ptr++;
        n |= ((unsigned long long)*data->ptr++) << 8;
        n |= ((unsigned long long)*data->ptr++) << 16;
        n |= ((unsigned long long)*data->ptr++) << 24;
        n |= ((unsigned long long)*data->ptr++) << 32;
        n |= ((unsigned long long)*data->ptr++) << 40;
        n |= ((unsigned long long)*data->ptr++) << 48;
        n |= ((unsigned long long)*data->ptr++) << 56;
        return n;
    default:
        return v;
    }
}

static VALUE lcb(VALUE obj)
{
    data_t *data;
    unsigned char v;
    unsigned long long n;

    Data_Get_Struct(obj, data_t, data);
    n = _lcb(data);
    if (n == NIL_VALUE)
        return Qnil;
    return ULL2NUM(n);
}

static VALUE lcs(VALUE obj, VALUE len)
{
    data_t *data;
    unsigned long long l;
    VALUE ret;

    Data_Get_Struct(obj, data_t, data);
    l = _lcb(data);
    if (l == NIL_VALUE)
        return Qnil;
    if (data->ptr+l > data->endp)
        l = data->endp - data->ptr;
    ret = rb_str_new(data->ptr, l);
    data->ptr += l;
    return ret;
}

static VALUE read(VALUE obj, VALUE len)
{
    data_t *data;
    unsigned long long  l = NUM2ULL(len);
    VALUE ret;

    Data_Get_Struct(obj, data_t, data);
    if (data->ptr+l > data->endp)
        l = data->endp - data->ptr;
    ret = rb_str_new(data->ptr, l);
    data->ptr += l;
    return ret;
}

static VALUE string(VALUE obj)
{
    data_t *data;
    unsigned char *p;
    VALUE ret;

    Data_Get_Struct(obj, data_t, data);
    p = data->ptr;
    while (p < data->endp && *p++ != '\0')
        ;
    ret = rb_str_new(data->ptr, (p - data->ptr)-1);
    data->ptr = p;
    return ret;
}

static VALUE utiny(VALUE obj)
{
    data_t *data;

    Data_Get_Struct(obj, data_t, data);
    return UINT2NUM(*data->ptr++);
}

static VALUE _ushort(VALUE obj)
{
    data_t *data;
    unsigned short n;

    Data_Get_Struct(obj, data_t, data);
    n = *data->ptr++;
    n |= *data->ptr++ * 0x100;
    return UINT2NUM(n);
}

static VALUE _ulong(VALUE obj)
{
    data_t *data;
    unsigned long n;

    Data_Get_Struct(obj, data_t, data);
    n = *data->ptr++;
    n |= *data->ptr++ * 0x100;
    n |= *data->ptr++ * 0x10000;
    n |= *data->ptr++ * 0x1000000;
    return UINT2NUM(n);
}

static VALUE eofQ(VALUE obj)
{
    data_t *data;

    Data_Get_Struct(obj, data_t, data);
    if (*data->ptr == 0xfe && data->endp - data->ptr == 5)
        return Qtrue;
    else
        return Qfalse;
}

static VALUE to_s(VALUE obj)
{
    data_t *data;

    Data_Get_Struct(obj, data_t, data);
    return rb_str_new(data->ptr, data->endp-data->ptr);
}

void Init_packet(void)
{
    VALUE cMysql;
    VALUE cPacket;

    cMysql = rb_define_class("Mysql", rb_cObject);
    cPacket = rb_define_class_under(cMysql, "Packet", rb_cObject);
    rb_define_alloc_func(cPacket, allocate);
    rb_define_singleton_method(cPacket, "lcb", s_lcb, 1);
    rb_define_singleton_method(cPacket, "lcs", s_lcs, 1);
    rb_define_method(cPacket, "initialize", initialize, 1);
    rb_define_method(cPacket, "lcb", lcb, 0);
    rb_define_method(cPacket, "lcs", lcs, 0);
    rb_define_method(cPacket, "read", read, 1);
    rb_define_method(cPacket, "string", string, 0);
    rb_define_method(cPacket, "utiny", utiny, 0);
    rb_define_method(cPacket, "ushort", _ushort, 0);
    rb_define_method(cPacket, "ulong", _ulong, 0);
    rb_define_method(cPacket, "eof?", eofQ, 0);
    rb_define_method(cPacket, "to_s", to_s, 0);
}
