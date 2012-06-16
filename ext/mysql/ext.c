#include "ruby.h"

typedef struct {
    unsigned char *ptr;
    unsigned char *endp;
} packet_data_t;

static VALUE cMysql;
static VALUE cPacket;
static VALUE cMysqlTime;
static VALUE cProtocol;
static VALUE cStmtRawRecord;
static VALUE cCharset;
static VALUE eProtocolError;

static VALUE packet_s_lcb(VALUE klass, VALUE val)
{
    unsigned long long n;
    unsigned char buf[9];
    int i;

    if (val == Qnil)
        return rb_str_new("\xfb", 1);
    n = NUM2ULL(val);
    if (n < 251) {
        buf[0] = n;
        return rb_str_new(buf, 1);
    }
    if (n < 65536) {
        buf[0] = '\xfc';
#ifdef WORDS_BIGENDIAN
        buf[1] = n % 256;
        buf[2] = n / 256;
#else
        memcpy(&buf[1], (char *)&n, 2);
#endif
        return rb_str_new(buf, 3);
    }
    if (n < 16777216) {
        buf[0] = '\xfd';
#ifdef WORDS_BIGENDIAN
        buf[1] = n % 256;
        n /= 256;
        buf[2] = n % 256;
        buf[3] = n / 256;
#else
        memcpy(&buf[1], (char *)&n, 3);
#endif
        return rb_str_new(buf, 4);
    }
    buf[0] = '\xfe';
#ifdef WORDS_BIGENDIAN
    for (i = 0; i < 8; i++) {
        buf[i+1] = *((char *)&n + 7-i);
    }
#else
    memcpy(&buf[1], (char *)&n, 8);
#endif
    return rb_str_new(buf, 9);
}

static VALUE packet_s_lcs(VALUE klass, VALUE val)
{
    VALUE ret = packet_s_lcb(klass, ULONG2NUM(RSTRING_LEN(val)));
    return rb_str_cat(ret, RSTRING_PTR(val), RSTRING_LEN(val));
}

static VALUE packet_allocate(VALUE klass)
{
    packet_data_t *data;

    data = xmalloc(sizeof *data);
    data->ptr = NULL;
    data->endp = NULL;
    return Data_Wrap_Struct(klass, 0, xfree, data);
}

static VALUE packet_initialize(VALUE obj, VALUE buf)
{
    packet_data_t *data;

    Data_Get_Struct(obj, packet_data_t, data);
    rb_ivar_set(obj, rb_intern("buf"), buf);
    data->ptr = RSTRING_PTR(buf);
    data->endp = data->ptr + RSTRING_LEN(buf);
}

#define NIL_VALUE 0xFFFFFFFFFFFFFFFF

static unsigned long long _packet_lcb(packet_data_t *data)
{
    unsigned char v;
    unsigned long long n = 0;

    if (data->ptr >= data->endp)
        return NIL_VALUE;

    v = *data->ptr++;
    switch (v) {
    case 0xfb:
        return NIL_VALUE;
    case 0xfc:
#ifdef WORDS_BIGENDIAN
        n = *data->ptr++;
        n |= ((unsigned int)*data->ptr++) << 8;
#else
        memcpy((char *)&n, data->ptr, 2);
        data->ptr += 2;
#endif
        return n;
    case 0xfd:
#ifdef WORDS_BIGENDIAN
        n = *data->ptr++;
        n |= ((unsigned int)*data->ptr++) << 8;
        n |= ((unsigned int)*data->ptr++) << 16;
#else
        memcpy((char *)&n, data->ptr, 3);
        data->ptr += 3;
#endif
        return n;
    case 0xfe:
#ifdef WORDS_BIGENDIAN
        n = *data->ptr++;
        n |= ((unsigned long long)*data->ptr++) << 8;
        n |= ((unsigned long long)*data->ptr++) << 16;
        n |= ((unsigned long long)*data->ptr++) << 24;
        n |= ((unsigned long long)*data->ptr++) << 32;
        n |= ((unsigned long long)*data->ptr++) << 40;
        n |= ((unsigned long long)*data->ptr++) << 48;
        n |= ((unsigned long long)*data->ptr++) << 56;
#else
        memcpy((char *)&n, data->ptr, 8);
        data->ptr += 8;
#endif
        return n;
    default:
        return v;
    }
}

static VALUE packet_lcb(VALUE obj)
{
    packet_data_t *data;
    unsigned char v;
    unsigned long long n;

    Data_Get_Struct(obj, packet_data_t, data);
    n = _packet_lcb(data);
    if (n == NIL_VALUE)
        return Qnil;
    return ULL2NUM(n);
}

static VALUE packet_lcs(VALUE obj)
{
    packet_data_t *data;
    unsigned long long l;
    VALUE ret;

    Data_Get_Struct(obj, packet_data_t, data);
    l = _packet_lcb(data);
    if (l == NIL_VALUE)
        return Qnil;
    if (data->ptr+l > data->endp)
        l = data->endp - data->ptr;
    ret = rb_str_new(data->ptr, l);
    data->ptr += l;
    return ret;
}

static VALUE packet_read(VALUE obj, VALUE len)
{
    packet_data_t *data;
    unsigned long long  l = NUM2ULL(len);
    VALUE ret;

    Data_Get_Struct(obj, packet_data_t, data);
    if (data->ptr+l > data->endp)
        l = data->endp - data->ptr;
    ret = rb_str_new(data->ptr, l);
    data->ptr += l;
    return ret;
}

static VALUE packet_string(VALUE obj)
{
    packet_data_t *data;
    unsigned char *p;
    VALUE ret;

    Data_Get_Struct(obj, packet_data_t, data);
    p = data->ptr;
    while (p < data->endp && *p++ != '\0')
        ;
    ret = rb_str_new(data->ptr, (p - data->ptr)-1);
    data->ptr = p;
    return ret;
}

static VALUE packet_utiny(VALUE obj)
{
    packet_data_t *data;

    Data_Get_Struct(obj, packet_data_t, data);
    return UINT2NUM(*data->ptr++);
}

static unsigned short _packet_ushort(packet_data_t *data)
{
    unsigned short n;

#ifdef WORDS_BIGENDIAN
    n = *data->ptr++;
    n |= *data->ptr++ * 0x100;
    Data_Get_Struct(obj, packet_data_t, data);
#else
    memcpy((char *)&n, data->ptr, 2);
    data->ptr += 2;
#endif
    return n;
}

static VALUE packet_ushort(VALUE obj)
{
    packet_data_t *data;
    unsigned short n;

    Data_Get_Struct(obj, packet_data_t, data);
    n = _packet_ushort(data);
    return UINT2NUM(n);
}

static VALUE packet_ulong(VALUE obj)
{
    packet_data_t *data;
    unsigned long n;

    Data_Get_Struct(obj, packet_data_t, data);
#ifdef WORDS_BIGENDIAN
    n = *data->ptr++;
    n |= *data->ptr++ * 0x100;
    n |= *data->ptr++ * 0x10000;
    n |= *data->ptr++ * 0x1000000;
#else
    memcpy((char *)&n, data->ptr, 4);
    data->ptr += 4;
#endif
    return UINT2NUM(n);
}

static VALUE packet_eofQ(VALUE obj)
{
    packet_data_t *data;

    Data_Get_Struct(obj, packet_data_t, data);
    if (*data->ptr == 0xfe && data->endp - data->ptr == 5)
        return Qtrue;
    else
        return Qfalse;
}

static VALUE packet_to_s(VALUE obj)
{
    packet_data_t *data;

    Data_Get_Struct(obj, packet_data_t, data);
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

#define UNSIGNED_FLAG 32
#define BINARY_CHARSET_NUMBER 63

static VALUE protocol_net2value(VALUE klass, VALUE pkt, VALUE type, VALUE unsigned_flag)
{
    packet_data_t *data;
    unsigned long n;
    unsigned long long ll;
    float f;
    double fd;
    int len;
    int sign;
    unsigned long y, m, d, h, mi, s, bs;
    unsigned char buf[12];
    int uflag = (unsigned_flag != Qnil && unsigned_flag != Qfalse);

    Data_Get_Struct(pkt, packet_data_t, data);
    switch (FIX2INT(type)) {
    case TYPE_STRING:
    case TYPE_VAR_STRING:
    case TYPE_NEWDECIMAL:
    case TYPE_BLOB:
        return rb_funcall(pkt, rb_intern("lcs"), 0);
    case TYPE_TINY:
        n = *data->ptr++;
        return uflag ? INT2FIX(n) : INT2FIX((char)n);
    case TYPE_SHORT:
    case TYPE_YEAR:
        n = *data->ptr++;
        n |= *data->ptr++ * 0x100;
        return uflag ? INT2FIX(n) : INT2FIX((short)n);
    case TYPE_INT24:
    case TYPE_LONG:
        n = *data->ptr++;
        n |= *data->ptr++ * 0x100;
        n |= *data->ptr++ * 0x10000;
        n |= *data->ptr++ * 0x1000000;
        return uflag ? UINT2NUM(n) : INT2NUM((long)n);
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
        return uflag ? ULL2NUM(ll) : LL2NUM((long long)(ll));
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
        return rb_funcall(cMysqlTime, rb_intern("new"), 8, ULONG2NUM(y), ULONG2NUM(m), ULONG2NUM(d), ULONG2NUM(h), ULONG2NUM(mi), ULONG2NUM(s), Qfalse, ULONG2NUM(bs));
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

static VALUE protocol_value2net(VALUE klass, VALUE obj)
{
    int type;
    VALUE val;
    if (obj == Qnil) {
        type = TYPE_NULL;
        val = rb_str_new("", 0);
    } else if (FIXNUM_P(obj)) {
        long n;
        int flag = 0;
        char buf[sizeof(long)], buf2[sizeof(long)];

        n = FIX2LONG(obj);
        if (n >= 0) {
            flag = 0x8000;
        }
        memcpy(buf, (char *)&n, sizeof(n));
#ifdef WORDS_BIGENDIAN
        for (i=0; i<sizeof(n); i++) {
            buf2[i] = buf[sizeof(n)-i];
        }
        memcpy(buf, buf2, sizeof(buf));
#endif
        if (-0x80 <= n && n < 0x100) {
            type = TYPE_TINY | flag;
            val = rb_str_new(buf, 1);
        } else if (-0x8000 <= n && n < 0x10000) {
            type = TYPE_SHORT | flag;
            val = rb_str_new(buf, 2);
#if SIZEOF_LONG == 4
        } else {
            type = TYPE_LONG | flag;
            val = rb_str_new(buf, 4);
#else
        } else if (-0x80000000 <= n && n < 0x100000000) {
            type = TYPE_LONG | flag;
            val = rb_str_new(buf, 4);
        } else {
            type = TYPE_LONGLONG | flag;
            val = rb_str_new(buf, 8);
#endif
        }
    } else if (TYPE(obj) == T_BIGNUM) {
        char buf[sizeof(long long)], buf2[sizeof(long long)];
        if (RBIGNUM_SIGN(obj)) {
            unsigned long long ull;
            ull = NUM2ULL(obj);
            memcpy(buf, (char *)&ull, sizeof(ull));
            type = TYPE_LONGLONG | 0x8000;
        } else {
            long long ll;
            ll = NUM2LL(obj);
            memcpy(buf, (char *)&ll, sizeof(ll));
            type = TYPE_LONGLONG;
        }
#ifdef WORDS_BIGENDIAN
        for (i=0; i<sizeof(buf); i++) {
            buf2[i] = buf[sizeof(buf)-i];
        }
        memcpy(buf, buf2, sizeof(buf));
#endif
        val = rb_str_new(buf, sizeof(buf));
    } else if (rb_obj_is_kind_of(obj, rb_cFloat)) {
        double dbl;

        dbl = NUM2DBL(obj);
        type = TYPE_DOUBLE;
        val = rb_str_new((char *)&dbl, sizeof(dbl));
    } else if (rb_obj_is_kind_of(obj, rb_cString)) {
        type = TYPE_STRING;
        val = packet_s_lcs(0, obj);
    } else if (rb_obj_is_kind_of(obj, cMysqlTime) || rb_obj_is_kind_of(obj, rb_cTime)) {
        int year, month, day, hour, min, sec;
        char buf[8];

        year = FIX2INT(rb_funcall(obj, rb_intern("year"), 0));
        month = FIX2INT(rb_funcall(obj, rb_intern("month"), 0));
        day = FIX2INT(rb_funcall(obj, rb_intern("day"), 0));
        hour = FIX2INT(rb_funcall(obj, rb_intern("hour"), 0));
        min = FIX2INT(rb_funcall(obj, rb_intern("min"), 0));
        sec = FIX2INT(rb_funcall(obj, rb_intern("sec"), 0));
        type = TYPE_DATETIME;
        buf[0] = 7;
        buf[1] = year & 0xff;
        buf[2] = (year >> 8) & 0xff;
        buf[3] = month;
        buf[4] = day;
        buf[5] = hour;
        buf[6] = min;
        buf[7] = sec;
        val = rb_str_new(buf, 8);
    } else {
        rb_raise(eProtocolError, "class %s is not supported", rb_class2name(rb_obj_class(obj)));
    }
    return rb_ary_new3(2, INT2FIX(type), val);
}

VALUE stmt_raw_record_parse_record_packet(VALUE obj)
{
    VALUE packet;
    VALUE fields;
    packet_data_t *data;
    int nfields;
    int bitmap_length;
    char *bitmap;
    int i;
    VALUE rec;

    packet = rb_iv_get(obj, "@packet");
    fields = rb_iv_get(obj, "@fields");
    Data_Get_Struct(packet, packet_data_t, data);
    data->ptr++;
    nfields = RARRAY_LEN(fields);
    bitmap_length = (nfields+7+2)/8;
    bitmap = data->ptr;
    data->ptr += bitmap_length;
    rec = rb_ary_new2(nfields);
    for (i = 0; i < nfields; i++) {
        if ((bitmap[(i+2)/8] >> (i+2)%8) & 1) {
            rb_ary_push(rec, Qnil);
        } else {
            VALUE field, u_flag, value;
            field = RARRAY_PTR(fields)[i];
            u_flag = (FIX2INT(rb_iv_get(field, "@flags")) & UNSIGNED_FLAG) == 0 ? Qfalse : Qtrue;
            value = protocol_net2value(cProtocol, packet, rb_iv_get(field, "@type"), u_flag);
            if (rb_obj_is_kind_of(value, rb_cNumeric) || rb_obj_is_kind_of(value, cMysqlTime)) {
                rb_ary_push(rec, value);
            } else if (FIX2INT(rb_iv_get(field, "@type")) == TYPE_BIT || FIX2INT(rb_iv_get(field, "@charsetnr")) == BINARY_CHARSET_NUMBER) {
                rb_ary_push(rec, rb_funcall(cCharset, rb_intern("to_binary"), 1, value));
            } else {
                rb_ary_push(rec, rb_funcall(cCharset, rb_intern("convert_encoding"), 2, value, rb_iv_get(obj, "@encoding")));
            }
        }
    }
    return rec;
}

void Init_ext(void)
{
    cMysql = rb_const_get(rb_cObject, rb_intern("Mysql"));
    cPacket = rb_const_get(cMysql, rb_intern("Packet"));
    cMysqlTime = rb_define_class_under(cMysql, "Time", rb_cObject);
    cProtocol = rb_const_get(cMysql, rb_intern("Protocol"));
    cStmtRawRecord = rb_const_get(cMysql, rb_intern("StmtRawRecord"));
    cCharset = rb_const_get(cMysql, rb_intern("Charset"));
    eProtocolError = rb_const_get(cMysql, rb_intern("ProtocolError"));

    rb_define_alloc_func(cPacket, packet_allocate);
    rb_define_singleton_method(cPacket, "lcb", packet_s_lcb, 1);
    rb_define_singleton_method(cPacket, "lcs", packet_s_lcs, 1);
    rb_define_method(cPacket, "initialize", packet_initialize, 1);
    rb_define_method(cPacket, "lcb", packet_lcb, 0);
    rb_define_method(cPacket, "lcs", packet_lcs, 0);
    rb_define_method(cPacket, "read", packet_read, 1);
    rb_define_method(cPacket, "string", packet_string, 0);
    rb_define_method(cPacket, "utiny", packet_utiny, 0);
    rb_define_method(cPacket, "ushort", packet_ushort, 0);
    rb_define_method(cPacket, "ulong", packet_ulong, 0);
    rb_define_method(cPacket, "eof?", packet_eofQ, 0);
    rb_define_method(cPacket, "to_s", packet_to_s, 0);

    rb_define_singleton_method(cProtocol, "net2value", protocol_net2value, 3);
    rb_define_singleton_method(cProtocol, "value2net", protocol_value2net, 1);

    rb_define_method(cStmtRawRecord, "parse_record_packet", stmt_raw_record_parse_record_packet, 0);
    rb_define_alias(cStmtRawRecord, "to_a", "parse_record_packet");
}
