import type {
	ClientOptions,
	TLSOptions,
} from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { ConnectionParamsError } from "https://deno.land/x/postgres@v0.19.3/client/error.ts";
import { parseConnectionUri } from "https://deno.land/x/postgres@v0.19.3/utils/utils.ts";
import { isAbsolute } from "https://deno.land/std@0.214.0/path/is_absolute.ts";

/** https://www.postgresql.org/docs/14/libpq-ssl.html#LIBPQ-SSL-PROTECTION */
type TLSModes = "disable" | "prefer" | "require" | "verify-ca" | "verify-full";

interface PostgresUri {
	application_name?: string;
	dbname?: string;
	driver: string;
	host?: string;
	options?: string;
	password?: string;
	port?: string;
	sslmode?: TLSModes;
	user?: string;
}

function parseOptionsArgument(options: string): Record<string, string> {
	const args = options.split(" ");

	const transformed_args = [];
	for (let x = 0; x < args.length; x++) {
		if (/^-\w/.test(args[x])) {
			if (args[x] === "-c") {
				if (args[x + 1] === undefined) {
					throw new Error(
						`No provided value for "${args[x]}" in options parameter`,
					);
				}

				// Skip next iteration
				transformed_args.push(args[x + 1]);
				x++;
			} else {
				throw new Error(
					`Argument "${args[x]}" is not supported in options parameter`,
				);
			}
		} else if (/^--\w/.test(args[x])) {
			transformed_args.push(args[x].slice(2));
		} else {
			throw new Error(`Value "${args[x]}" is not a valid options argument`);
		}
	}

	return transformed_args.reduce(
		(options, x) => {
			if (!/.+=.+/.test(x)) {
				throw new Error(`Value "${x}" is not a valid options argument`);
			}

			const key = x.slice(0, x.indexOf("="));
			const value = x.slice(x.indexOf("=") + 1);

			options[key] = value;

			return options;
		},
		{} as Record<string, string>,
	);
}

export async function parseOptionsFromUri(
	connection_string: string,
	caFilePaths?: string[],
): Promise<ClientOptions> {
	let postgres_uri: PostgresUri;
	try {
		const uri = parseConnectionUri(connection_string);
		postgres_uri = {
			application_name: uri.params.application_name,
			dbname: uri.path || uri.params.dbname,
			driver: uri.driver,
			host: uri.host || uri.params.host,
			options: uri.params.options,
			password: uri.password || uri.params.password,
			port: uri.port || uri.params.port,
			sslmode:
				uri.params.ssl === "true"
					? "require"
					: (uri.params.sslmode as TLSModes),
			user: uri.user || uri.params.user,
		};
	} catch (e) {
		throw new ConnectionParamsError("Could not parse the connection string", e);
	}

	if (!["postgres", "postgresql"].includes(postgres_uri.driver)) {
		throw new ConnectionParamsError(
			`Supplied DSN has invalid driver: ${postgres_uri.driver}.`,
		);
	}

	const host_type = postgres_uri.host
		? isAbsolute(postgres_uri.host)
			? "socket"
			: "tcp"
		: "socket";

	const options = postgres_uri.options
		? parseOptionsArgument(postgres_uri.options)
		: {};

	let tls: TLSOptions | undefined;
	switch (postgres_uri.sslmode) {
		case undefined: {
			break;
		}
		case "disable": {
			tls = { enabled: false, enforce: false, caCertificates: [] };
			break;
		}
		case "prefer": {
			tls = { enabled: true, enforce: false, caCertificates: [] };
			break;
		}
		case "require":
		case "verify-ca":
		case "verify-full": {
			const caCertificates = [];
			if (caFilePaths) {
				for (const path of caFilePaths) {
					caCertificates.push(await Deno.readTextFile(path));
				}
			}
			tls = { enabled: true, enforce: true, caCertificates };
			break;
		}
		default: {
			throw new ConnectionParamsError(
				`Supplied DSN has invalid sslmode '${postgres_uri.sslmode}'`,
			);
		}
	}

	return {
		applicationName: postgres_uri.application_name,
		database: postgres_uri.dbname,
		hostname: postgres_uri.host,
		host_type,
		options,
		password: postgres_uri.password,
		port: postgres_uri.port,
		tls,
		user: postgres_uri.user,
	};
}
