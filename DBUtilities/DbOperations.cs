using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;


namespace DBUtilities
{
    public class DbOperations
    {
        public static string _cadenaConexion { get; protected set; }
        public static string _proveedorDatos { get; protected set; }

        public DbOperations(string cadenaConexion, string proveedorDatos)
        {
            _cadenaConexion = cadenaConexion;
            _proveedorDatos = proveedorDatos;
        }

        /// -------------------------------------------------------------------
        /// Ejecuta comando de consulta regresando el no. de registros afectadas
        /// Comandos INSERT, UPDATE, DELETE
        /// 
        public int EjecutaComandoNonQuery(string cmdSQL, Dictionary<string, object> parametros = null)
        {
            return EjecutaComandoNonQuery(_cadenaConexion, _proveedorDatos, cmdSQL, parametros);
        }

        public static int EjecutaComandoNonQuery(string cadenaConexion, string proveedorDatos, string consulta, Dictionary<string, object> parametros = null)
        {
            DbProviderFactory fabrica = DbProviderFactories.GetFactory(proveedorDatos);

            using (DbConnection conn = fabrica.CreateConnection())
            {
                conn.ConnectionString = cadenaConexion;

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = consulta;

                    if (parametros != null)
                    {
                        foreach (KeyValuePair<string, object> kvp in parametros)
                        {
                            DbParameter parametro = fabrica.CreateParameter();
                            parametro.ParameterName = kvp.Key;
                            parametro.Value = kvp.Value;
                            cmd.Parameters.Add(parametro);
                        }
                    }

                    try
                    {
                        conn.Open();
                        return cmd.ExecuteNonQuery();
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine(e.Message);
                        return 0;
                    }
                }
            }
        }

        /// -------------------------------------------------------------------
        /// Ejecuta comando regresando la primera columna del primer registro encontrado
        /// Comandos: SELECT
        /// 
        public int SeleccionaUltimoRegistro(string cmdSQL)
        {
            return SeleccionaUltimoRegistro(_cadenaConexion, _proveedorDatos, cmdSQL);
        }
        public static int SeleccionaUltimoRegistro(string cadenaConexion, string proveedorDatos, string consulta)
        {
            DbProviderFactory fabrica = DbProviderFactories.GetFactory(proveedorDatos);
            using (DbConnection conn = fabrica.CreateConnection())
            {
                conn.ConnectionString = cadenaConexion;
                using (DbCommand cmd = conn.CreateCommand())
                using (conn)
                {
                    cmd.CommandText = consulta;
                    conn.Open();
                    object rc = cmd.ExecuteScalar();
                    if (rc == null)
                    {
                        return 1;
                    }
                    else
                    {
                        return Convert.ToInt32(rc);
                    }
                }
            }
        }

        /// -------------------------------------------------------------------
        /// Ejecuta comando regresando los registros resultantes del query
        /// Comandos: SELECT
        /// 
        public DataTable Seleccion(string consulta, Dictionary<string, object> parametros = null)
        {
            return Seleccion(_cadenaConexion, _proveedorDatos, consulta, parametros);
        }

        public static DataTable Seleccion(string cadenaConexion, string proveedorDatos, string consulta, Dictionary<string, object> parametros = null)
        {
            DbProviderFactory fabrica = DbProviderFactories.GetFactory(proveedorDatos);
            DataTable dt = new DataTable();

            using (DbConnection conn = fabrica.CreateConnection())
            {
                conn.ConnectionString = cadenaConexion;
                using (DbCommand cmd = conn.CreateCommand())
                using (DbDataAdapter da = fabrica.CreateDataAdapter())
                {
                    cmd.CommandText = consulta;
                    da.SelectCommand = cmd;

                    if (parametros != null)
                    {
                        foreach (KeyValuePair<string, object> kvp in parametros)
                        {
                            DbParameter parametro = cmd.CreateParameter();
                            parametro.ParameterName = kvp.Key;
                            parametro.Value = kvp.Value;
                            cmd.Parameters.Add(parametro);
                        }
                    }
                    try
                    {
                        conn.Open();
                        da.Fill(dt);
                        return dt;
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine(e.Message);
                        return null;
                    }
                }
            }
        }

        /// -------------------------------------------------------------------
        /// Ejecuta comando regresando el registro recién agregado
        /// Comandos: SELECT
        /// 
        public int SeleccionaReciente(string consulta)
        {
            return SeleccionaReciente(_cadenaConexion, _proveedorDatos, consulta);
        }

        public static int SeleccionaReciente(string cadenaConexion, string proveedorDatos, string consulta)
        {
            DbProviderFactory fabrica = DbProviderFactories.GetFactory(proveedorDatos);

            using (DbConnection conn = fabrica.CreateConnection())
            {
                conn.ConnectionString = cadenaConexion;
                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = consulta;
                    try
                    {
                        conn.Open();
                        return (Int16)cmd.ExecuteScalar();
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine(e.Message);
                        return 0;
                    }
                }
            }
        }

        /// -------------------------------------------------------------------
        /// Ejecuta comando regresando el ID del registro recién agregado
        /// Comandos INSERT, UPDATE, DELETE
        /// 
        public int EjecutaComandoScalar(string cmdSQL, Dictionary<string, object> parametros = null)
        {
            return EjecutaComandoScalar(_cadenaConexion, _proveedorDatos, cmdSQL, parametros);
        }

        public static int EjecutaComandoScalar(string cadenaConexion, string proveedorDatos, string consulta, Dictionary<string, object> parametros = null)
        {
            DbProviderFactory fabrica = DbProviderFactories.GetFactory(proveedorDatos);

            using (DbConnection conn = fabrica.CreateConnection())
            {
                conn.ConnectionString = cadenaConexion;

                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = consulta;

                    if (parametros != null)
                    {
                        foreach (KeyValuePair<string, object> kvp in parametros)
                        {
                            DbParameter parametro = fabrica.CreateParameter();
                            parametro.ParameterName = kvp.Key;
                            parametro.Value = kvp.Value;
                            cmd.Parameters.Add(parametro);
                        }
                    }

                    try
                    {
                        conn.Open();
                        return (Int32) cmd.ExecuteScalar();
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine(e.Message);
                        return 0;
                    }
                }
            }
        }

        /// -------------------------------------------------------------------
        /// Ejecuta comando regresando el campo especificado en la consulta
        /// Comandos: SELECT
        ///
        public object SeleccionaCampo(string consulta)
        {
            return SeleccionaCampo(_cadenaConexion, _proveedorDatos, consulta);
        }
        public static object SeleccionaCampo(string cadenaConexion, string proveedorDatos, string consulta)
        {
            DbProviderFactory fabrica = DbProviderFactories.GetFactory(proveedorDatos);

            using (DbConnection conn = fabrica.CreateConnection())
            {
                conn.ConnectionString = cadenaConexion;
                using (DbCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = consulta;
                    try
                    {
                        conn.Open();
                        return cmd.ExecuteScalar();
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine(e.Message);
                        return "";
                    }
                }
            }
        }
    }
}
