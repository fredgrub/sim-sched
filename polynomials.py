def lin(x, t0, t1, t2, t3):
    p, q, r = x
    lin_term = (t0 + t1*p + t2*q + t3*r)
    return lin_term

def sqr(x, t0, t1, t2, t3, t4, t5, t6, t7):
    p, q, r = x
    lin_term = lin(x, t0, t1, t2, t3)
    sq_term = (t4*p**2 + t5*q**2 + t6*r**2) + (t7*p*q)

    return lin_term + sq_term

def cub(
    x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
    t10, t11, t12):
    p, q, r = x
    sq_term = sqr(x, t0, t1, t2, t3, t4, t5, t6, t7)
    cub_term = (t8*p**3 + t9*q**3 + t10*r**3) \
        + (t11*(p**2)*q + t12*p*(q**2))

    return sq_term + cub_term

def qua(
    x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
    t10, t11, t12, t13, t14, t15, t16, t17, t18):
    p, q, r = x
    cub_term = cub(
        x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
        t10, t11, t12)
    qua_term = (t13*p**4 + t14*q**4 + t15*r**4) \
        + (t16*(p**3)*q + t17*(p*q)**2 + t18*p*(q**3))
    
    return cub_term + qua_term

def qui(
    x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
    t10, t11, t12, t13, t14, t15, t16, t17, t18, t19,
    t20, t21, t22, t23, t24, t25):
    p, q, r = x
    qua_term = qua(
        x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
        t10, t11, t12, t13, t14, t15, t16, t17, t18)
    qui_term = (t19*p**5 + t20*q**5 + t21*r**5) \
        + (t22*(p**4)*q + t23*(p**3)*(q**2) + t24*(p**2)*(q**3) \
            + t25*p*(q**4))

    return qua_term + qui_term

def sex(
    x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
    t10, t11, t12, t13, t14, t15, t16, t17, t18, t19,
    t20, t21, t22, t23, t24, t25, t26, t27, t28, t29,
    t30, t31, t32, t33):
    p, q, r = x
    qui_term = qui(
        x, t0, t1, t2, t3, t4, t5, t6, t7, t8, t9,
        t10, t11, t12, t13, t14, t15, t16, t17, t18, t19,
        t20, t21, t22, t23, t24, t25)
    sex_term = (t26*p**6 + t27*q**6 + t28*r**6) \
        + (t29*(p**5)*q + t30*(p**4)*(q**2) + t31*(p*q)**3 \
            + t32*(p**2)*(q**4) + t33*p*(q**5))

    return qui_term + sex_term