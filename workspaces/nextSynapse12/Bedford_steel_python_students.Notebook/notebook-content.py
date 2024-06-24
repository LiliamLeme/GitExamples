# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "67f8983e-c811-4672-9b76-77704bf6075a",
# META       "default_lakehouse_name": "SQLDW",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Solving linear problems in Python
# 
# **Author: Jessica Cervi**
# 
# -----
# 
# ## Description of the problem:
# 
# In this notebook, we will show you how to implement and solve an optimisation problem in Python.
# 
# For this example, we will consider again the Bedford steel problem presented in the lectures.
# 
# The Bedforf steel problem can be regarded as an optimisation problem with 8 degrees of freedom (DOF) $x_0,x_1,x_2,x_3,x_4,x_5,x_6,x_7$, where we are trying to minimise the function
# 
# $$49500x_0 + 50000x_1 +61000x_2 + 63500x_3 +66500x_4 +71000x_5+72500x_6 +80000x_7,$$
# 
# subject to the following contraints:
# 
# $$x_0+x_1+x_2+x_3+x_4+x_5+x_6+x_7 = 1225,$$
# 
# $$x_0+x_1+x_3+x_5 \geq 612.5,$$
# 
# $$x_0+x_2+x_6+x_7 \leq 650,$$
# 
# $$x_1+x_3+x_4+x_5 \leq720,$$
# 
# and 
# $$0.15x_0+0.16x_1+0.18x_2+0.2x_3+0.21x_4+0.22x_5+0.23x_6+0.25x_7 \geq 232.75.$$
# 
# Additionally, each DOF is contained within the bounds:
# $$ 0.0 \leq x_0 \leq 300,$$
# $$ 0.0 \leq x_1 \leq 600,$$
# $$ 0.0 \leq x_2 \leq 510,$$
# $$ 0.0 \leq x_3 \leq 655,$$
# $$ 0.0 \leq x_4 \leq 575,$$
# $$ 0.0 \leq x_5 \leq 680,$$
# $$ 0.0 \leq x_6 \leq 450,$$
# $$ 0.0 \leq x_7 \leq 490.$$
# 
# We choose the initial condition,  $i_0$, to be origin: 
# $$i_0 = [0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0].$$
# 
# Note that because we have 8 DOF, the origin is also defined to have 8 points.

# MARKDOWN ********************

# ## Using Python to solve an optimization problem
# 
# Python is a very powerful and versatile programming language that allows you to solve many different type of problems.
# 
# Optimization problems can be solved in Python using many different libraries, such as [PuLP](https://pypi.org/project/PuLP/), [Gurobi](https://www.gurobi.com/downloads/?campaignid=193283256&adgroupid=51266130904&creative=406108478817&keyword=%2Bgurobi&matchtype=b&gclid=Cj0KCQjwwr32BRD4ARIsAAJNf_1Ra_nFioCaWFJTX6cTA8d-5KqQ0DXFY28XR-JuGsOyl-4yOH79ShwaAtyDEALw_wcB) or [SciPy optimize](https://docs.scipy.org/doc/scipy/reference/optimize.html). 
# 
# For sake of simplicity, in this tutorial we will use the `SciPy` implemetation of the solver. `SciPy optimize` provides functions for minimising (or maximising) objective functions, possibly subject to constraints. It includes solvers for nonlinear problems (with support for both local and global optimisation algorithms), linear programing, constrained and nonlinear least-squares, among other things.
# 
# Because the Bedford problem is a **local, multivariate** problem, we will need to use the function `minimize` from `SciPy optimize`.
# 
# Complete the code cell below by importing `minimize` from `scipy.optmize`.

# CELL ********************

spark.catalog.tableExists("SQLDW.factinternetsales")

# CELL ********************

+1

# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.factinternetsales LIMIT 1000")
display(df)

# CELL ********************

!pip install SciPy

# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.ISS_Stream_Fabric LIMIT 1000")
display(df)

# CELL ********************

#complete the code below
import scipy.optimize

# MARKDOWN ********************

# Now that we have imported the necesseray library, we are ready to start defining our optmisation problem.
# 
# 
# #### Defining the objective function
# 
# We will begin by defining the objective function
# 
# $$49500x_0 + 50000x_1 +61000x_2 + 63500x_3 +66500x_4 +71000x_5+72500x_6 +80000x_7$$
# 
# This can be done by defining a function, in this case called `objective_fun`, that contains the definition of the function we want to minimise. Note that this function takes one argument, `x`, that is an array containing our DOF `x[0]`,`x[1]`,`x[2]`,`x[3]`,`x[4]`,`x[5]`,`x[6]`,`x[7]`.
# 
# In the code cell below, we have defined the function header for you. Complete the function definition by writing the objective function after the `return` statement in a similar way as follows:
# 
# ```Python
# def objective_fun(x):
#     return 49500*x[0] + 50000*x[1] + ...
# ```
# 


# CELL ********************

#Complete the function definition
def objective_fun(x): 
    return 49500*x[0] + 50000*x[1] + 61000*x[2] +63500*x[3] +66500*x[4] +71000*x[5] + 72500*x[6] + 80000*x[7]

# MARKDOWN ********************

# #### Defining the constraints
# 
# `SciPy optimize` can handle both linear and non-linear constraints, with equalities or inequalities. We need to be little careful with how we define the constrains in Python. In fact, a constraint function $f(x)$ needs to be written as 
# 
# $$f(x) = 0,$$
# 
# in case of an equality, or as 
# 
# $$f(x) \geq 0,$$
# in case of an inequality. Therefore, sometime we might need to manipulate the equations.
# 
# Additionally, each constraint needs to be defined in a separate function. For this problem, we will have to define then 5 constraint functions.
# 
# Run the code cell below where we have defined the first contraint for you in the function `constraint1`.

# CELL ********************

def constraint1(x):
    sum_con1 = 1225
    for i in range(7):
         sum_con1 = sum_con1 - x[i]
    return sum_con1

# MARKDOWN ********************

# Next, we will define the second constraint.
# 
# In the code cell below, we have defined the function header of `constraint2` for you.  Complete the function definition by writing the second constraint after the return statement in a similar way as follows:
# 
# ```Python
# def constraint2(x):
#     return x[0] + .... - 612.5
# ```

# CELL ********************

#Complete the function definition
def constraint2(x):
    return x[0] +x[1] +x[3] +x[5] -612.5




# MARKDOWN ********************

# In the code cells below, complete the definitions of the remaining constraints as follows:
# 
#     
# ```Python
# def constraint3(x):
#     return 650 - x[0] - ...
# ```
# 
# 
# ```Python
# def constraint4(x):
#     return 720 - x[1] - ..
# ```
# 
# 
# ```Python
# def constraint5(x):
#     return 0.15*x[0] + 0.16*x[1] + ... - 232.75
# ```
#     

# CELL ********************

#Complete the function definition
def constraint3(x):
    return x[0] +x[2] +x[6] +x[7] - 650


# CELL ********************

#Complete the function definition
def constraint4(x):
    return x[1] +x[3] +x[4] +x[5] - 720

# CELL ********************

#Complete the function definition
def constraint5(x):
    return 0.15*x[0] + 0.16*x[1] +0.18*x[2]+0.2*x[3]+0.21*x[4]+0.22*x[5]+0.23*x[6]+0.25*x[7] -232.75

# MARKDOWN ********************

# Great! We are alomost done with the constraints. The last thing we need to do is to tell Python the type of each constraint: equality `eq` or inequality `ineq`.
# 
# In the code cell below, we have completed the definition of the first constraint be defining a dictonary, `con1`, with keys `type` and `fun` and values `eq` and `constraint1`, respectively.
# 
# Run the code cell below.

# CELL ********************

con1 = {"type": "eq", "fun" : constraint1}

# MARKDOWN ********************

# In a similar way, in the code cell below, complete the defition on the remaining constraints. 
# 
# **HINT: Notice that now these constraints are inequalities!**

# CELL ********************

#Complete the constraint definition
con2 = {"type": "eq", "fun" : constraint2}
con3 = {"type": "eq", "fun" : constraint3}
con4 = {"type": "eq", "fun" : constraint4}
con5 ={"type": "eq", "fun" : constraint5}

# MARKDOWN ********************

# FInally, we collect all these dictonaries inside a list, `cons`, for future use.
# 
# Run the code cell below.

# CELL ********************

cons = [con1, con2, con3, con4, con5]

# MARKDOWN ********************

# #### Definition of the bounds
# 
# In this step, we will define the bounds for each of the DOF as defined at the beginning of this notebook.
# 
# In the code cell below, we have defined in a tuple `b_0` the lower and upper bounds for `x_0`.
# 
# Run the code cell below

# CELL ********************

b_0 = (0.0, 300.0)

# MARKDOWN ********************

# In a similar way, in the code cell below, complete the definition on the remaining bounds for the remaining DOF. 
# 
# **HINT: Notice that we have used floats!**

# CELL ********************

#Complete the bounds definition
b_1 = (0.0, 600.0)
b_2 = (0.0, 510.0)
b_3 = (0.0, 655.0)
b_4 = (0.0, 575.0)
b_5 = (0.0, 680.0)
b_6 = (0.0, 450.0)
b_7 = (0.0, 490.0)

# MARKDOWN ********************

# FInally, we collect all these tuples as an array, `bnds`, for future use.
# 
# Run the code cell below.

# CELL ********************

bnds = (b_0, b_1, b_2, b_3, b_4, b_5, b_6, b_7)

# MARKDOWN ********************

# #### Definition of the initial guess
# 
# Run the code cell below to define the initial guess `i_0`.

# CELL ********************

i_0 = [0,0,0,0,0,0,0,0]

# MARKDOWN ********************

# #### Solving the optimization problem
# 
# Now we are ready to solve our problem. As mentioned earlier, we will use the function `SciPy optimize` `minimize()` with the following arguments:
# 
# - `objective_fun`: the name of the function where we have defined the fuction that we want to minimise.
# - `i_0`: the initial condition.
# - `method = SLSPQ`: the optimization algorith chosen to solve the problem. You can find a comprehesinve list of the options available [here](https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.minimize.html).
# - `bounds = bnds`: the bounds for our problem.
# - `constraints = cons`: the constraints of our problem.
# 
# Run the code cell below to solve our problem!

# CELL ********************

from scipy.optimize import minimize
sol = minimize(objective_fun,  i_0, method='SLSQP', bounds = bnds, constraints = cons)

# MARKDOWN ********************

# #### Visualizing the solution
# 
# We can visualize some information about our solution by using the command `print(sol)`.
# 
# Run the code below.

# CELL ********************

print(sol)

# MARKDOWN ********************

# You observe that `sol` is an object representing the optimization results. This description, among other things, includes a feedback about wheter the algorith ran successfully or not, the minimization function (jac) and so on.
# 
# The solution vector `x` is also part of these attributes. Notice that it contains the optimal solution found by the solver and that (as expected!) matches the one we found by solving the same exercise in Excel.
# 
# For a complete description of the attributes returned please see [here](https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.OptimizeResult.html#scipy.optimize.OptimizeResult).

# MARKDOWN ********************

# **CONGRATULATIONS ON COMPLETING THIS ACTIVITY!**

# CELL ********************

