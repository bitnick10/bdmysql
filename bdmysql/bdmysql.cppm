#include <boost/describe.hpp>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <type_traits>
#include <SQLiteCpp/SQLiteCpp.h>
#include <iostream>
#include <mysqlx/xdevapi.h>

export module bdmysql;

export namespace bdmysql {

inline std::string insert_value(int v) { return fmt::format("{0}", v); }
inline std::string insert_value(float v) { return fmt::format("{0}", v); }
inline std::string insert_value(double v) { return fmt::format("{0}", v); }
inline std::string insert_value(const std::string& v) { return fmt::format("\"{0}\"", v); }
inline std::string insert_value(const std::chrono::system_clock::time_point& v) { return fmt::format("FROM_UNIXTIME({0})", std::chrono::duration_cast<std::chrono::seconds>(v.time_since_epoch()).count()); }

inline std::string insert_value(const std::optional<int>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
inline std::string insert_value(const std::optional<float>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
inline std::string insert_value(const std::optional<double>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
inline std::string insert_value(const std::optional<std::string>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }
inline std::string insert_value(const std::optional<std::chrono::system_clock::time_point>& v) { if (v.has_value()) { return insert_value(v.value()); } else { return "NULL"; } }

inline void get_cell(int& des, const mysqlx::Value& v) { if (!v.isNull()) des = v.get<int>(); }
inline void get_cell(float& des, const mysqlx::Value& v) { if (!v.isNull()) des = v.get<float>(); }
inline void get_cell(double& des, const mysqlx::Value& v) { if (!v.isNull()) des = v.get<double>(); }
inline void get_cell(std::string& des, const mysqlx::Value& v) { if (!v.isNull()) des = v.get<std::string>(); }
inline void get_cell(std::chrono::system_clock::time_point& des, const mysqlx::Value& v) { if (!v.isNull()) des = std::chrono::system_clock::from_time_t(time_t{ 0 }) + std::chrono::seconds(v.get<int>()); }

inline void get_cell(std::optional<int>& des, const mysqlx::Value& v) { if (!v.isNull()) { des = v.get<int>(); } }
inline void get_cell(std::optional<float>& des, const mysqlx::Value& v) { if (!v.isNull()) { des = v.get<float>(); } }
inline void get_cell(std::optional<double>& des, const mysqlx::Value& v) { if (!v.isNull()) { des = v.get<double>(); } }
inline void get_cell(std::optional<std::string>& des, const mysqlx::Value& v) { if (!v.isNull()) { des = v.get<std::string>(); } }
inline void get_cell(std::optional<std::chrono::system_clock::time_point>& des, const mysqlx::Value& v) { if (!v.isNull()) { des = std::chrono::system_clock::from_time_t(time_t{ 0 }) + std::chrono::seconds(v.get<int>()); } }


template<typename T>
inline std::vector<std::string> column_names() {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;

	std::vector<std::string> ret;

	T t;
	boost::mp11::mp_for_each<Md>([&](auto D) {
		std::string colname = D.name;
	if (colname.back() == '_') colname.pop_back();
	if (typeid(t.*D.pointer) == typeid(std::chrono::system_clock::time_point) || typeid(t.*D.pointer) == typeid(std::optional<std::chrono::system_clock::time_point>)) {
		ret.push_back(fmt::format("UNIX_TIMESTAMP({0})", colname));
	}
	else
		ret.push_back(colname);
		});
	return ret;
}

inline std::string set_str(const std::string& name, int v) { return fmt::format("{0}={1}", name, v); }
inline std::string set_str(const std::string& name, float v) { return fmt::format("{0}={1}", name, v); }
inline std::string set_str(const std::string& name, double v) { return fmt::format("{0}={1}", name, v); }
inline std::string set_str(const std::string& name, const std::string& v) { return fmt::format("{0}=\"{1}\"", name, v); }
inline std::string set_str(const std::string& name, const std::chrono::system_clock::time_point& v) { return fmt::format("{0}=FROM_UNIXTIME({1})", name, std::chrono::duration_cast<std::chrono::seconds>(v.time_since_epoch()).count()); }

inline std::string set_str(const std::string& name, const std::optional<int>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<float>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<double>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<std::string>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }
inline std::string set_str(const std::string& name, const std::optional<std::chrono::system_clock::time_point>& v) { if (v.has_value()) { return set_str(name, v.value()); } else { return ""; } }


template<typename T>
void update_sets(const T& t, std::string& sets) {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;
	int i = 0;
	boost::mp11::mp_for_each<Md>([&](auto D) {
		std::string colname = D.name;
	if (colname.back() == '_') colname.pop_back();
	std::string set = set_str(colname, t.*D.pointer);
	if (!set.empty()) {
		sets += set + ",";
	}
		});
	if (sets.back() == ',') sets.pop_back();
}

template<typename T>
void primary_column_assignments(const T& t, const std::vector<std::string>& primary_keys, std::string& condition) {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;
	int i = 0;
	boost::mp11::mp_for_each<Md>([&](auto D) {
		std::string colname = D.name;
	if (colname.back() == '_') colname.pop_back();
	if (std::find(primary_keys.begin(), primary_keys.end(), colname) != primary_keys.end())
	{
		i++;
		condition += colname;
		condition += "=" + insert_value(t.*D.pointer);
		if (condition.back() == ',') condition.pop_back();
		if (i < primary_keys.size()) condition += " AND ";
	}
		});
}

template<typename T>
void read_row(T& t, mysqlx::Row& row) {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;
	int i = 0;
	boost::mp11::mp_for_each<Md>([&](auto D) {
		get_cell(t.*D.pointer, row[i]);
	i++;
		});
}

template<typename T>
inline std::string GetCreateTableQuery(const std::string& tableName, const std::string& primaryKey, const std::string& engine) {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;

	std::string query = fmt::format("CREATE TABLE if not exists {0} (", tableName);

	boost::mp11::mp_for_each<Md>([&](auto D) {
		std::string colname = D.name;
	if (colname.back() == '_') colname.pop_back();
	query += colname;
	std::string fullname = typeid(D.pointer).name();
	std::string coltype;

	if (fullname.rfind("int", 0) == 0) { coltype = "INT"; }
	if (fullname.rfind("float", 0) == 0) { coltype = "FLOAT"; }
	if (fullname.rfind("double", 0) == 0) { coltype = "DOUBLE"; }
	if (fullname.rfind("class std::basic_string", 0) == 0) { coltype = "VARCHAR(31)"; }
	if (fullname.rfind("class std::chrono::time_point", 0) == 0) { coltype = "TIMESTAMP"; }

	if (fullname.rfind("class std::optional<int>", 0) == 0) { coltype = "INT"; }
	if (fullname.rfind("class std::optional<float>", 0) == 0) { coltype = "FLOAT"; }
	if (fullname.rfind("class std::optional<double>", 0) == 0) { coltype = "DOUBLE"; }
	if (fullname.rfind("class std::optional<class std::basic_string", 0) == 0) { coltype = "VARCHAR(31)"; }
	if (fullname.rfind("class std::optional<class std::chrono::time_point", 0) == 0) { coltype = "TIMESTAMP"; }

	query += " " + coltype + ",";
		});
	query += "PRIMARY KEY (" + primaryKey + "))";
	if (!engine.empty())
		query += " ENGINE=" + engine;
	query += ";";
	return query;
}

template<typename T>
void CreateTableIfNotExists(mysqlx::Session& sess, const std::string& tableName, const std::string& primaryKey, const std::string& engine) {
	std::string query;
	try {
		query = GetCreateTableQuery<T>(tableName, primaryKey, engine);
		sess.sql(query).execute();
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from CreateTableI:\n";
		std::cout << query << "\n";
	}
}

template<typename T>
inline std::string ToInsertString(const T& t, const std::string& tableName, const std::string& insertType = "INSERT INTO") {
	using namespace boost::describe;

	using Bd = describe_bases<T, mod_any_access>;
	using Md = describe_members<T, mod_any_access>;

	std::string query = fmt::format("{0} {1} VALUES(", insertType, tableName);
	boost::mp11::mp_for_each<Md>([&](auto D) {
		query += insert_value(t.*D.pointer) + ",";
		});
	query.pop_back();
	query += ");";
	return query;
}

template<typename T>
inline void ReplaceInto(mysqlx::Session& sess, const std::string& tableName, const T& value) {
	std::string query;
	try {
		query = ToInsertString(value, tableName, "REPLACE INTO");
		sess.sql(query).execute();
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from ReplaceInto:\n";
		std::cout << query << "\n";
	}
}

template<typename T>
inline void ReplaceInto(mysqlx::Session& sess, const std::string& tableName, const std::vector<T>& rows) {
	std::string query;
	try {
		sess.startTransaction();
		for (auto& value : rows) {
			query = ToInsertString(value, tableName, "REPLACE INTO");
			sess.sql(query).execute();
		}
		sess.commit();
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from ReplaceInto:\n";
		std::cout << query << "\n";
	}
}

template<typename T>
std::vector<T> Select(mysqlx::Session& sess, const std::string& tableName, const std::string& condition = "") {
	std::vector<std::string> names = column_names<T>();
	std::string selects = "SELECT ";
	for (auto& name : names) {
		selects += name + ",";
	}
	selects.pop_back();
	auto query = fmt::format("{0} FROM {1}", selects, tableName);
	if (!condition.empty()) { query += " WHERE " + condition; }

	std::vector<T> ret;

	try {
		auto res = sess.sql(query).execute();
		std::list<mysqlx::Row> rows = res.fetchAll();
		for (auto& row : rows) {
			T object;
			read_row(object, row);
			ret.push_back(object);
		}
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from Select:\n";
		std::cout << query << "\n";
		throw e;
	}
	return ret;
}

template<typename T>
void Update(mysqlx::Session& sess, const T& object, const std::string& tableName, const std::vector<std::string>& primary_keys, const std::string& condition = "") {
	std::string query;
	try {
		std::string sets;
		update_sets<T>(object, sets);
		std::string condition;
		primary_column_assignments(object, primary_keys, condition);
		query = fmt::format("UPDATE {0} SET {1} WHERE {2}", tableName, sets, condition);
		sess.sql(query).execute();
	}
	catch (std::exception e) {
		std::cout << e.what() << "\n" << "error from Update:\n";
		std::cout << query << "\n";
	}
}

bool table_exists(mysqlx::Session& sess, const std::string schema, const std::string& table) {
	sess.sql("USE " + schema).execute();
	auto query = fmt::format("SELECT* FROM information_schema.tables WHERE table_schema = '{0}' AND table_name = '{1}' LIMIT 1;", schema, table);
	auto res = sess.sql(query).execute();
	return res.count() != 0;
}

}
