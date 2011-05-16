<?php
/**
 * MogileFS Client
 *
 * @package MogileFS
 * @category Exceptions
 * @author Kiall Mac Innes
 * @copyright (c) 2011 Managed I.T.
 * @license http://www.managedit.ie/opensource/license
 */
class MogileFS
{
	/**
	 *
	 * @var Config 
	 */
	protected $_config = NULL;

	/**
	 *
	 * @var resource
	 */
	protected $_socket = NULL;
	
	/**
	 * @return  MogileFS
	 */
	public static function instance()
	{
		static $instance;

		if ($instance == NULL)
		{
			$instance = new MogileFS();
		}

		return $instance;
	}

	public function __construct()
	{
		$this->_config = Kohana::config('mogilefs');
		$this->connect();
	}

	public function set($key, $content, $class = NULL)
	{
		$class = ($class === NULL) ? $this->_config->default_class : $class;

		$location = $this->_do_request('CREATE_OPEN', array(
			'key'	=> $key,
			'domain' => $this->_config->domain,
			'class'  => $class,
		));

		$response = Request::factory($location['path'])
			->method('PUT')
			->headers('Content-Length', (string) strlen($content))
			->body($content)
			->execute();

		if ($response->status() < 200 OR $response->status() > 299)
			throw new MogileFS_Exception('Server returned a :status status code', array(
				':status' => $response->status(),
			));

		$this->_do_request('CREATE_CLOSE', array(
			'key'	=> $key,
			'domain' => $this->_config->domain,
			'devid'  => $location['devid'],
			'fid'	=> $location['fid'],
			'path'   => $location['path'],
		));
	}

	public function set_file($key, $file, $class = NULL)
	{
		$fh = fopen($file, 'r');
		$fs = filesize($file);

		return $this->set_resource($key, $fh, $class, $fs);
	}

	public function set_resource($key, $rh, $class = NULL, $length = NULL)
	{
		$class = ($class === NULL) ? $this->_config->default_class : $class;

		$location = $this->_do_request('CREATE_OPEN', array(
			'key'	=> $key,
			'domain' => $this->_config->domain,
			'class'  => $class,
		));

		$request = Request::factory($location['path'])
			->method('PUT')
			->body($rh);

		if ($length !== NULL)
		{
			$request->headers('Content-Length', (string) $length);
		}
		
		$response = $request->execute();

		if ($response->status() < 200 OR $response->status() > 299)
			throw new MogileFS_Exception('Server returned a :status status code', array(
				':status' => $response->status(),
			));

		$this->_do_request('CREATE_CLOSE', array(
			'key'	=> $key,
			'domain' => $this->_config->domain,
			'devid'  => $location['devid'],
			'fid'	=> $location['fid'],
			'path'   => $location['path'],
		));
	}

	public function get($key)
	{
		$paths = $this->get_paths($key, TRUE);

		foreach ($paths as $path)
		{
			$response = Request::factory($path)->execute();

			if ($response->status() != 200)
				continue;

			return $response->body();
		}

		throw new MogileFS_Exception("Unable to retrieve key ':key'", array(
			':key' => $key,
		));
	}

	public function get_paths($key, $verify = TRUE)
	{
		$result = $this->_do_request('GET_PATHS', array(
			'key'	  => $key,
			'noverify' => (int) (bool) ! $verify,
			'domain'   => $this->_config->domain,
		));

		// "paths" contains the number of paths returned.. Its not really necessary and messes with foreach() etc
		unset($result['paths']);

		return $result;
	}
	
	public function rename($from_key, $to_key)
	{
		$this->_do_request('RENAME', array(
			'from_key' => $from_key,
			'to_key'   => $to_key,
			'domain'   => $this->_config->domain,
		));

		return $this;
	}

	public function delete($key)
	{
		$this->_do_request('DELETE', array(
			'key'	=> $key,
			'domain' => $this->_config->domain,
		));

		return $this;
	}

	public function connected()
	{
		return ($this->_socket && is_resource($this->_socket) && !feof($this->_socket));
	}

	public function connect()
	{
		if ($this->connected())
			return $this;

		foreach ($this->_config->trackers as $tracker)
		{
			$parts = parse_url($tracker);
			$errno = null;
			$errstr = null;

			try
			{
				$this->_socket = fsockopen(
					$parts['host'],
					isset($parts['port']) ? $parts['port'] : 7001,
					$errno,
					$errstr,
					$this->_config->connect_timeout
				);

				if ($this->_socket)
				{
					Kohana::$log->add(Log::DEBUG, "MogileFS: Successfully connected to ':tracker'.", array(
						':tracker' => $tracker,
					));

					stream_set_timeout(
						$this->_socket,
						floor($this->_config->tracker_timeout),
						($this->_config->tracker_timeout - floor($this->_config->tracker_timeout)) * 1000
					);

					return $this;
				}
			}
			catch (ErrorException $e)
			{
				Kohana::$log->add(Log::NOTICE, "MogileFS: Unable to connect to ':tracker'. Errno: :errno. Errstr: :errstr.", array(
					':tracker' => $tracker,
					':errno'   => $errno,
					':errstr'  => $errstr,
				));
				
				continue;
			}
		}

		throw new MogileFS_Exception('Unable to obtain connection to any tracker.');
	}

	public function disconnect()
	{
		if ( ! $this->connected())
			return $this;

		fclose($this->_socket);

		$this->_socket = NULL;
	}

	protected function _do_request($cmd, array $args = array())
	{
		if ( ! $this->connected())
			$this->connect();
		
		$params = '';

		foreach ($args as $key => $value)
		{
			$params .= '&' . urlencode($key) . '=' . urlencode($value);
		}

		$result = fwrite($this->_socket, $cmd . $params . "\n");

		if ($result === FALSE)
		{
			$this->disconnect();
			
			throw new MogileFS_Exception('Unable to write to socket');
		}

		$line = fgets($this->_socket);

		if ($line === FALSE)
		{
			$this->disconnect();
			
			throw new MogileFS_Exception('Unable to read from socket');
		}

		$words = explode(' ', $line);

		if ($words[0] == 'OK')
		{
			parse_str(trim($words[1]), $result);

			return $result;
		}
		else if ($words[0] == 'ERR')
		{
			if (!isset($words[1]))
				$words[1] = NULL;

			switch ($words[1]) {
				case 'unknown_key':
					throw new MogileFS_Exception_UnknownKey("Unknown key ':key'", array(
						':key' => $args['key'],
					));

				case 'empty_file':
					throw new MogileFS_Exception_EmptyFile("Empty file ':key'", array(
						':key' => $args['key'],
					));

				case 'none_match':
					throw new MogileFS_Exception_NoneMatch("None match ':key'", array(
						':key' => $args['key'],
					));

				default:
					throw new MogileFS_Exception('Unknown Error: :message', array(
						':message' => trim(urldecode($line)),
					));
			}
		}
		else
		{
			// No idea what happened ...
			$this->disconnect();

			throw new MogileFS_Exception('Unknown error!');
		}
	}
}
