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

static VALUE lcs(VALUE obj)
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

enum {
    TYPE_DECIMAL     = 0,
    TYPE_TINY        = 1,
    TYPE_SHORT       = 2,
    TYPE_LONG        = 3,
    TYPE_FLOAT       = 4,
    TYPE_DOUBLE      = 5,
    TYPE_NULL        = 6,
    TYPE_TIMESTAMP   = 7,
    TYPE_LONGLONG    = 8,
    TYPE_INT24       = 9,
    TYPE_DATE        = 10,
    TYPE_TIME        = 11,
    TYPE_DATETIME    = 12,
    TYPE_YEAR        = 13,
    TYPE_NEWDATE     = 14,
    TYPE_VARCHAR     = 15,
    TYPE_BIT         = 16,
    TYPE_NEWDECIMAL  = 246,
    TYPE_ENUM        = 247,
    TYPE_SET         = 248,
    TYPE_TINY_BLOB   = 249,
    TYPE_MEDIUM_BLOB = 250,
    TYPE_LONG_BLOB   = 251,
    TYPE_BLOB        = 252,
    TYPE_VAR_STRING  = 253,
    TYPE_STRING      = 254,
    TYPE_GEOMETRY    = 255
};

static VALUE cMysqlTime;

static VALUE net2value(VALUE obj, VALUE pkt, VALUE type, VALUE unsigned_flag)
{
    data_t *data;
    unsigned long n;
    unsigned long long ll;
    float f;
    double fd;
    int len;
    int sign;
    unsigned long y, m, d, h, mi, s, bs;
    unsigned char buf[12];

    Data_Get_Struct(pkt, data_t, data);
    switch (FIX2INT(type)) {
    case TYPE_STRING:
    case TYPE_VAR_STRING:
    case TYPE_NEWDECIMAL:
    case TYPE_BLOB:
        return rb_funcall(pkt, rb_intern("lcs"), 0);
    case TYPE_TINY:
        n = *data->ptr++;
        return unsigned_flag ? INT2FIX(n) : INT2FIX((char)n);
    case TYPE_SHORT:
    case TYPE_YEAR:
        n = *data->ptr++;
        n |= *data->ptr++ * 0x100;
        return unsigned_flag ? INT2FIX(n) : INT2FIX((short)n);
    case TYPE_INT24:
    case TYPE_LONG:
        n = *data->ptr++;
        n |= *data->ptr++ * 0x100;
        n |= *data->ptr++ * 0x10000;
        n |= *data->ptr++ * 0x1000000;
        return unsigned_flag ? UINT2NUM(n) : INT2NUM((long)n);
    case TYPE_LONGLONG:
        n = *data->ptr++;
        n |= *data->ptr++ * 0x100;
        n |= *data->ptr++ * 0x10000;
        n |= *data->ptr++ * 0x1000000;
        ll = *data->ptr++;
        ll |= *data->ptr++ * 0x100;
        ll |= *data->ptr++ * 0x10000;
        ll |= *data->ptr++ * 0x1000000;
        ll = (ll<<32) + n;
        return unsigned_flag ? ULL2NUM(ll) : LL2NUM((long long)(ll));
    case TYPE_FLOAT:
        memcpy(&f, data->ptr, 4);
        data->ptr += 4;
        return rb_float_new(f);
    case TYPE_DOUBLE:
        memcpy(&fd, data->ptr, 8);
        data->ptr += 8;
        return rb_float_new(fd);
    case TYPE_DATE:
        len = *data->ptr++;
        memset(buf, 0, sizeof(buf));
        memcpy(buf, data->ptr, len);
        data->ptr += len;
        y = buf[0] | buf[1]<<8;
        m = buf[2];
        d = buf[3];
        return rb_funcall(cMysqlTime, rb_intern("new"), 6, ULONG2NUM(y), ULONG2NUM(m), ULONG2NUM(d), Qnil, Qnil, Qnil);
    case TYPE_DATETIME:
    case TYPE_TIMESTAMP:
        len = *data->ptr++;
        memset(buf, 0, sizeof(buf));
        memcpy(buf, data->ptr, len);
        data->ptr += len;
        y = buf[0] | buf[1]<<8;
        m = buf[2];
        d = buf[3];
        h = buf[4];
        mi = buf[5];
        s = buf[6];
        bs = buf[7] | buf[8]<<8 | buf[9]<<16 | buf[10]<<24;
        return rb_funcall(cMysqlTime, rb_intern("new"), 7, ULONG2NUM(y), ULONG2NUM(m), ULONG2NUM(d), ULONG2NUM(h), ULONG2NUM(mi), ULONG2NUM(s), ULONG2NUM(bs));
    case TYPE_TIME:
        len = *data->ptr++;
        memset(buf, 0, sizeof(buf));
        memcpy(buf, data->ptr, len);
        data->ptr += len;
        sign = buf[0];
        d = buf[1] | buf[2]<<8 | buf[3]<<16 | buf[4]<<24;;
        h = buf[5];
        mi = buf[6];
        s = buf[7];
        bs = buf[8] | buf[9]<<8 | buf[10]<<16 | buf[11]<<24;;
        h += d * 24;
        return rb_funcall(cMysqlTime, rb_intern("new"), 8, ULONG2NUM(0), ULONG2NUM(0), ULONG2NUM(0), ULONG2NUM(h), ULONG2NUM(mi), ULONG2NUM(s), (sign != 0 ? Qtrue : Qfalse), ULONG2NUM(bs));
    case TYPE_BIT:
        return rb_funcall(pkt, rb_intern("lcs"), 0);
    default:
        rb_raise(rb_eRuntimeError, "%s", "not implemented: type=#{%d}", FIX2INT(type));
    }
}

void Init_packet(void)
{
    VALUE cMysql;
    VALUE cPacket;
    VALUE cProtocol;

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

    cMysqlTime = rb_define_class_under(cMysql, "Time", rb_cObject);
    cProtocol = rb_define_class_under(cMysql, "Protocol", rb_cObject);
    rb_define_singleton_method(cProtocol, "net2value", net2value, 3);
}
