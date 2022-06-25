from scipy.optimize import curve_fit
import numpy as np

def lin(
    x, t0,
    t1, t2, t3
):
    p, q, r = x
    return (t0 + t1*p + t2*q + t3*r)

def sq(
    x, t0,
    t1, t2, t3,
    t4, t5, t6,
    t7
):
    p, q, r = x
    return (t0 + t1*p + t2*q + t3*r) + \
           (t4*p**2 + t5*q**2 + t6*r**2) + (t7*p*q)

def cub(
    x, t0,
    t1,  t2,  t3,
    t4,  t5,  t6,
    t7,  t8,  t9,
    t10, t11, t12
):
    p, q, r = x
    return (t0 + t1*p + t2*q + t3*r) + \
           (t4*p**2 + t5*q**2 + t6*r**2) + (t7*p*q) + \
           (t8*p**3 + t9*q**3 + t10*r**3) + (t11*(p**2)*q + t12*p*(q**2))

def qua(
    x, t0,
    t1,  t2,  t3,
    t4,  t5,  t6,
    t7,  t8,  t9,
    t10, t11, t12,
    t13, t14, t15,
    t16, t17, t18
):
    p, q, r = x
    return (t0 + t1*p + t2*q + t3*r) + \
           (t4*p**2 + t5*q**2 + t6*r**2) + (t7*p*q) + \
           (t8*p**3 + t9*q**3 + t10*r**3) + (t11*(p**2)*q + t12*p*(q**2)) + \
           (t13*p**4 + t14*q**4 + t15*r**4) + (t16*(p**3)*q + t17*p*(q**4) + t18*(p*q)**2)

def fit_curve(func, p, q, r, score, sample_weights):
    size = score.shape[0]
    popt, pcov = curve_fit(func, (p, q, r), score, sigma=sample_weights, absolute_sigma=True)
    
    residual = 0.0
    for i in range(size):
        residual += np.absolute(score[i] - func((p[i], q[i], r[i]), *popt))
    err = (residual/size)
    
    return popt, err
