/*
  returns the percentile of the values in a distribution 

  input parameters:
  data (real or int)
  desired percentile of result, 0-100 (int)
  decimals of output (optional, int)

  output:
  percentile value of the distribution (real)

  compiling
  gcc -fPIC -Wall -I /usr/include/mysql51/mysql/ -shared -o udf_percentile.so  udf_percentile.cc 
  udf_percentile.so /usr/lib64/mysql/plugin/

  registering the function:
  CREATE AGGREGATE FUNCTION percentile RETURNS REAL SONAME 'udf_percentile.so';

  getting rid of the function:
  DROP FUNCTION percentile;

  hat tip to Jan Steemann's udf_median function http://mysql-udf.sourceforge.net/
*/


#ifdef STANDARD
#include <stdio.h>
#include <string.h>
#ifdef __WIN__
typedef unsigned __int64 ulonglong;
typedef __int64 longlong;
#else
typedef unsigned long long ulonglong;
typedef long long longlong;
#endif /*__WIN__*/
#else
#include <my_global.h>
#include <my_sys.h>
#endif
#include <mysql.h>
#include <m_ctype.h>
#include <m_string.h>

#ifdef HAVE_DLOPEN


#define BUFFERSIZE 1024



extern "C" {
my_bool percentile_init( UDF_INIT* initid, UDF_ARGS* args, char* message );
void percentile_deinit( UDF_INIT* initid );
void percentile_reset( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error );
void percentile_clear(UDF_INIT* initid, char *is_null, char *error);
void percentile_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error );
double percentile( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error );
}


struct percentile_data
{
  unsigned long count;
  unsigned long abscount;
  unsigned long pages;
  double *values;
};


my_bool percentile_init( UDF_INIT* initid, UDF_ARGS* args, char* message )
{
  if ( args->arg_count < 2 || args->arg_count>3)
  {
    strcpy(message,"wrong number of arguments: percentile() requires two - three arguments");
    return 1;
  }

  if (args->arg_type[0]!=REAL_RESULT && args->arg_type[0]!=INT_RESULT)
  {
    strcpy(message,"percentile() requires a real or int as parameter 1");
    return 1;
  }

  if (args->arg_type[1]!=REAL_RESULT && args->arg_type[1]!=INT_RESULT)
  {
    asprintf(&message,"percentile() requires a real/int as parameter 2, instead %d",args->arg_type[1]);
    return 1;
  }
  if (args->arg_count>2 && args->arg_type[2]!=INT_RESULT)
  {
    strcpy(message,"percentile() requires an int as parameter 3");
    return 1;
  }

  initid->decimals=2;
  if (args->arg_count==3 && (*((ulong*)args->args[2])<=16))
  {
    initid->decimals=*((ulong*)args->args[2]);
  }

  percentile_data *buffer = new percentile_data;
  buffer->count = 0;
  buffer->abscount=0;
  buffer->pages = 1;
  buffer->values = NULL;

  initid->maybe_null    = 1;
  initid->max_length    = 32;
  initid->ptr = (char*)buffer;

  return 0;
}


void percentile_deinit( UDF_INIT* initid )
{
  percentile_data *buffer = (percentile_data*)initid->ptr;

  if (buffer->values != NULL)
  {
    free(buffer->values);
    buffer->values=NULL;
  }
  delete initid->ptr;
}

void percentile_clear(UDF_INIT* initid, char *is_null, char *error){
  percentile_data *buffer = (percentile_data*)initid->ptr;
  buffer->count = 0;
  buffer->abscount=0;
  buffer->pages = 1;
  *is_null = 0;
  *error = 0;

  if (buffer->values != NULL)
  {
    free(buffer->values);
    buffer->values=NULL;
  }

  buffer->values=(double *) malloc(BUFFERSIZE*sizeof(double));
}

void percentile_reset( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error )
{
  percentile_clear(initid, is_null, is_error);
  percentile_add( initid, args, is_null, is_error );
}


void percentile_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error )
{
  if (args->args[0]!=NULL)
  {
    percentile_data *buffer = (percentile_data*)initid->ptr;
    if (buffer->count>=BUFFERSIZE)
    {
      buffer->pages++;
      buffer->count=0;
      buffer->values=(double *) realloc(buffer->values,BUFFERSIZE*buffer->pages*sizeof(double));
    }
    if(args->arg_type[0]==INT_RESULT){
      buffer->values[buffer->abscount++] = (double) *((longlong*)args->args[0]);
    }    
    else{
      buffer->values[buffer->abscount++] = *((double*)args->args[0]);
    }
    buffer->count++;
  }
}

int compare_doubles (const void *a, const void *b)
{
  const double *da = (const double *) a;
  const double *db = (const double *) b;

  return (*da > *db) - (*da < *db);
}

double percentile( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error )
{
  percentile_data* buffer = (percentile_data*)initid->ptr;
  double pctile;

  if (buffer->abscount==0 || *is_error!=0)
  {
    *is_null = 1;
    return 0.0;
  }

  *is_null=0;
  if (buffer->abscount==1)
  {
    return buffer->values[0];
  }

  qsort(buffer->values,buffer->abscount,sizeof(double),compare_doubles);
  if(args->arg_type[1]==INT_RESULT){
    pctile = (double) *((longlong*)args->args[1]);
  }
  else{
    pctile = *((double*)args->args[1]);
  }

  int n = buffer->abscount - 1;
  
  if(pctile <=0){
    return buffer->values[0];
  }
  if(pctile>=100){
    return buffer->values[n];
  }
  double i = (n * pctile) / 100.0 ;
  int ix = (int) i;
  
  if (i == ix) 
  {
    return buffer->values[ix];
  } else
  {
    int j = ix + 1 >= n ? n : ix + 1;
    return(buffer->values[ix]*(j - i) + buffer->values[j]*(i - ix));
  }
}

#endif
