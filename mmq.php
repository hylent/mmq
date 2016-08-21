<?php

class Mmq_Exception extends Exception
{
}

class Mmq_SessionException extends Mmq_Exception
{
}

class Mmq_TubeException extends Mmq_Exception
{
}

class Mmq_Mysql
{
    protected $mysqli;

    public function __construct($mysqli)
    {
        $this->mysqli = $mysqli;
    }
}

class Mmq
{
    const DEFAULT_SESSION_TRIES = 3;
    const DEFAULT_SESSION_TTL   = 900;
    const DEFAULT_DEADLINE      = 1;
    const DEFAULT_TTR           = 30;
    const DEFAULT_PRIORITY      = 1024;
    const DEFAULT_TUBE          = 'default';

    const STATE_READY           = 0;
    const STATE_BURIED          = 1;
    const STATE_DELETED         = 2;

    protected $mysql;

    protected $sessionTries     = self::DEFAULT_SESSION_TRIES;
    protected $sessionTtl       = self::DEFAULT_SESSION_TTL;
    protected $deadline         = self::DEFAULT_DEADLINE;
    protected $ttr              = self::DEFAULT_TTR;
    protected $priority         = self::DEFAULT_PRIORITY;
    protected $tube             = self::DEFAULT_TUBE;

    protected $session;
    protected $sessionData;

    public function __construct(Mmq_Mysql $mysql, array $defaults = null, $session = '')
    {
        // Set mysql
        $this->mysql = $mysql;

        // Set defaults
        if (isset($defaults['sessionTries']) && is_int($defaults['sessionTries']) && $defaults['sessionTries'] > 0) {
            $this->sessionTries = $defaults['sessionTries'];
        }
        if (isset($defaults['sessionTtl']) && is_int($defaults['sessionTtl']) && $defaults['sessionTtl'] > 0) {
            $this->sessionTtl = $defaults['sessionTtl'];
        }
        if (isset($defaults['deadline']) && is_int($defaults['deadline']) && $defaults['deadline'] > 0) {
            $this->deadline = $defaults['deadline'];
        }
        if (isset($defaults['ttr']) && is_int($defaults['ttr']) && $defaults['ttr'] > 0) {
            $this->ttr = $defaults['ttr'];
        }
        if (isset($defaults['priority']) && is_int($defaults['priority']) && $defaults['priority'] > 0) {
            $this->priority = $defaults['priority'];
        }
        if (isset($defaults['tube']) && is_string($defaults['tube']) && strlen($defaults['tube']) > 0) {
            $this->tube = $defaults['tube'];
        }

        // Set session
        $session = (string) $session;
        if (preg_match('/^\d+$/', $session)) {
            $sessionData = $this->mysql->find('session', array(
                'session'   => $session,
            ));
            if (!$sessionData) {
                throw new Mmq_SessionException('Session '.$session.' not found');
            }
            $sessionData['watchedTubes'] = $this->mysql->findIndexedList('session', 'tube', array(
                'session'   => $session,
            ));
            $this->mysql->update('session', array(
                'ts_updated'    => time(),
            ), array(
                'session'       => $session,
            ));
        } else {
            for ($i = 1; $i <= $this->sessionTries; $i++) {
                $session = mt_rand(100000000, 999999999).mt_rand(100000000, 999999999);
                $sessionData = $this->mysql->find('session', array(
                    'session'   => $session,
                ));
                if ($sessionData) {
                    continue;
                }
                $sessionData = array(
                    'session'       => $session,
                    'tube'          => $this->tube,
                    'ts_created'    => time(),
                    'ts_updated'    => time(),
                );
                $this->mysql->insert('session', $sessionData);
                $this->mysql->insert('session_tube', array(
                    'session'       => $session,
                    'tube'          => $this->tube,
                ));
                $sessionData['watchedTubes'][$this->tube] = $this->tube;
            }
            throw new Mmq_SessionException('Failed starting session in '.$this->sessionTries.' tries');
        }
        $this->session      = $session;
        $this->sessionData  = $sessionData;
    }

    protected function filterTube($tube)
    {
        $tube = (string) $tube;

        if ($tube === '') {
            throw new Mmq_TubeException('Empty tube');
        }
        if (strlen($tube) > 200) {
            throw new Mmq_TubeException('Invalid tube '.$tube);
        }

        return $tube;
    }

    public function listTubes()
    {
        return $this->mysql->findList('job', 'distinct session', array(
            'state$ne'  => self::STATE_DELETED,
        ));
    }

    public function useTube($tube)
    {
        $tube = $this->filterTube($tube);

        if ($this->sessionData['tube'] == $tube) {
            return;
        }

        $this->mysql->update('session', array(
            'tube'          => $tube,
        ), array(
            'session'       => $session,
            'ts_updated'    => time(),
        ));
    }

    public function getUsedTube()
    {
        return $this->sessionData['tube'];
    }

    public function put($data, $ttr = 0, $pri = 0, $delay = 0)
    {
        $data = (string) $data;

        $ttr = (int) $ttr;
        if ($ttr < 1) {
            $ttr = $this->ttr;
        }

        $pri = (int) $pri;
        if ($pri < 1) {
            $pri = $this->pri;
        }

        $delay  = max(0, (int) $delay);

        $now = time();

        return $this->mysql->insert('job', array(
            'data'          => $data,
            'ttr'           => $ttr,
            'pri'           => $pri,
            'tube'          => $this->sessionData['tube'],
            'state'         => self::STATE_READY,
            'session'       => 0,
            'ts_available'  => $now + $delay,
            'ts_created'    => $now,
            'ts_updated'    => $now,
        ));
    }

    public function watchTube($tube)
    {
        $tube = $this->filterTube($tube);

        if (!isset($this->sessionData['watchedTubes'][$tube])) {
            $this->mysql->insert('session_tube', array(
                'session'   => $session,
                'tube'      => $tube,
            ));
            $this->sessionData['watchedTubes'][$tube] = $tube;
        }

        return count($this->sessionData['watchedTubes']);
    }

    public function ignoreTube($tube)
    {
        $tube = $this->filterTube($tube);

        $cnt = count($this->sessionData['watchedTubes']);

        if (isset($this->sessionData['watchedTubes'][$tube])) {
            if ($cnt == 1) {
                throw new Mmq_Exception_Tube('Last watched tube cannot be ignored');
            }

            $this->mysql->delete('session_tube', array(
                'session'   => $session,
                'tube'      => $tube,
            ));
            unset($this->sessionData['watchedTubes'][$tube]);
            $cnt--;
        }

        return $cnt;
    }

    public function listWatchedTubes()
    {
        return $this->sessionData['watchedTubes'];
    }

    /**
     * Consumer. Reserve a job from the queue.
     *
     * @param   long    timeout
     *
     * @return  null    when timed out
     * @return  array
     *          .id     long    job id
     *          .data   string  job data
     *          .tube   string  tube
     *
     * @throws  ERR_DEADLINE_SOON
     */
    public function reserve($timeout = 0)
    {
    }

    /**
     * Consumer. Delete a job of mine or nobody from the queue.
     *
     * @param   long    id
     *
     * @return  boolean false when not found
     */
    public function delete($id)
    {
    }

    /**
     * Consumer. Touch a job of mine.
     *
     * @param   long    id
     *
     * @return  boolean false when not found
     */
    public function touch($id)
    {
    }

    /**
     * Consumer. Release a job of mine back into the queue.
     *
     * @param   long    id
     * @param   long    delay
     * @param   long    pri
     *
     * @return  boolean false when not found
     */
    public function release($id, $delay, $pri)
    {
    }

    /**
     * Consumer. Bury a job of mine or nobody.
     *
     * @param   long    id
     * @param   long    pri
     *
     * @return  boolean false when not found
     */
    public function bury($id, $pri)
    {
    }

    /**
     * Client. Move a buried job into the ready state.
     *
     * @param   long    id
     *
     * @return  boolean false when not found
     */
    public function kick($id)
    {
    }

    /**
     * Client. Move buried jobs into the ready state on the currently used tube.
     *
     * @param   long    bound
     *
     * @return  long    jobs kicked
     */
    public function kickJobs($bound)
    {
    }

}
