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

class Mmq_JobException extends Mmq_Exception
{
}

class Mmq_Mysql
{
    protected $mysqli;

    public function __construct($mysqli)
    {
        $this->mysqli = $mysqli;
    }

    public function begin()
    {
    }

    public function commit()
    {
    }

    public function rollback()
    {
    }

    public function find()
    {
    }

    public function findForUpdate()
    {
    }

    public function findList()
    {
    }

    public function findIndexedList()
    {
    }

    public function delete()
    {
    }

    public function insert()
    {
    }

    public function update()
    {
    }
}

class Mmq
{
    const DEFAULT_SESSION_TRIES = 3;
    const DEFAULT_SESSION_TTL   = 900;
    const DEFAULT_TTR           = 90;
    const DEFAULT_PRIORITY      = 1024;
    const DEFAULT_TUBE          = 'default';

    const STATE_READY           = 0;
    const STATE_BURIED          = 1;
    const STATE_DELETED         = 2;

    protected $mysql;

    protected $sessionTries     = self::DEFAULT_SESSION_TRIES;
    protected $sessionTtl       = self::DEFAULT_SESSION_TTL;
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
        if (isset($defaults['ttr']) && is_int($defaults['ttr']) && $defaults['ttr'] > 0) {
            $this->ttr = $defaults['ttr'];
        }
        if (isset($defaults['priority']) && is_int($defaults['priority']) && $defaults['priority'] >= 0) {
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

    protected function filterTtr($ttr)
    {
        $ttr = (int) $ttr;

        return $ttr < 1 ? self::DEFAULT_TTR : $ttr;
    }

    protected function filterPriority($pri)
    {
        $pri = (int) $pri;

        return $pri < 0 ? self::DEFAULT_PRIORITY : $pri;
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

    protected function filterData($data)
    {
        $data = (string) $data;
        if (mb_strlen($data, 'utf-8') > 2000) {
            throw new Mmq_JobException('Job data too large');
        }

        return $data;
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

    public function put($data, $ttr = 0, $pri = -1, $delay = 0)
    {
        $data = $this->filterData($data);

        $ttr = $this->filterTtr($ttr);
        $pri = $this->filterPriority($pri);

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

    public function kickJobs($bound)
    {
        return $this->mysql->update('job', array(
            'session'       => 0,
            'state'         => self::STATE_READY,
            'ts_updated'    => $now,
        ), array(
            'state'         => self::STATE_BURIED,
            'tube'          => $this->sessionData['tube'],
        ), array(
            'pri'           => 'asc',
            'ts_available'  => 'asc',
            'id'            => 'asc',
        ), $bound);
    }

    public function kick($id)
    {
        $result = false;
        $this->mysql->begin();

        try {
            $job = $this->mysql->findForUpdate('job', array(
                'id'    => $id,
            ));

            if ($job && $job['state'] != self::STATE_DELETED) {
                $result = true;

                $now = time();

                if ($job['state'] == self::STATE_BURIED) {
                    throw new Mmq_JobException('Cannot kick a job not in buried state');
                }

                $this->mysql->update('job', array(
                    'session'       => 0,
                    'state'         => self::STATE_READY,
                    'ts_updated'    => $now,
                ), array(
                    'id'            => $id,
                ));
            }

        } catch (Exception $ex) {
            $this->mysql->rollback();
            throw $ex;
        }

        return $result;
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

    public function reserve($timeout = 0)
    {
        $expire = 1 + time() + max(0, (int) $timeout);

        $job = null;
        $this->mysql->begin();

        try {
            $sessionData = $this->mysql->findForUpdate('session', array(
                'session'   => $this->session,
            ));

            for ($i = 0; time() < $expire; $i++) {
                if ($i > 0) {
                    sleep(pow(2, $i - 1));
                }

                $now = time();

                $this->mysql->update('job', array(
                    'session'           => $this->session,
                    'ts_available$expr' => 'ttr + '.$now,
                    'ts_updated'        => $now,
                ), array(
                    'state'             => self::STATE_READY,
                    'tube$in'           => $this->sessionData['watchedTubes'],
                    'ts_available$lt'   => $now,
                ), array(
                    'pri'           => 'asc',
                    'ts_available'  => 'asc',
                    'id'            => 'asc',
                ), 1);

                $job = $this->mysql->find('job', array(
                    'session'       => $this->session,
                    'ts_updated'    => $now,
                ));

                if ($job) {
                    break;
                }
            }

            $this->mysql->commit();
        } catch (Exception $ex) {
            $this->mysql->rollback();
            throw $ex;
        }

        return $job;
    }

    public function delete($id)
    {
        $result = false;
        $this->mysql->begin();

        try {
            $job = $this->mysql->findForUpdate('job', array(
                'id'    => $id,
            ));

            if ($job && $job['state'] != self::STATE_DELETED) {
                $result = true;

                $now = time();

                if (
                    $job['state'] == self::STATE_READY
                    && $job['ts_available'] > $now
                    && $job['session']
                    && $job['session'] != $this->session
                ) {
                    throw new Mmq_JobException('Cannot delete job reserved by some other session');
                }

                $this->mysql->update('job', array(
                    'state'         => self::STATE_DELETED,
                    'ts_updated'    => $now,
                ), array(
                    'id'            => $id,
                ));
            }

        } catch (Exception $ex) {
            $this->mysql->rollback();
            throw $ex;
        }

        return $result;
    }

    public function touch($id)
    {
        $result = false;
        $this->mysql->begin();

        try {
            $job = $this->mysql->findForUpdate('job', array(
                'id'    => $id,
            ));

            if ($job && $job['state'] != self::STATE_DELETED) {
                $result = true;

                $now = time();

                if (
                    $job['state'] != self::STATE_READY
                    || $job['ts_available'] <= $now
                    || $job['session'] != $this->session
                ) {
                    throw new Mmq_JobException('Cannot touch job not reserved by this session');
                }

                $this->mysql->update('job', array(
                    'ts_available$expr' => 'ttr + '.$now,
                    'ts_updated'        => $now,
                ), array(
                    'id'                => $id,
                ));
            }

        } catch (Exception $ex) {
            $this->mysql->rollback();
            throw $ex;
        }

        return $result;
    }

    public function release($id, $pri = -1, $delay = 0)
    {
        $result = false;
        $this->mysql->begin();

        try {
            $job = $this->mysql->findForUpdate('job', array(
                'id'    => $id,
            ));

            if ($job && $job['state'] != self::STATE_DELETED) {
                $result = true;

                $now = time();

                if (
                    $job['state'] != self::STATE_READY
                    || $job['ts_available'] <= $now
                    || $job['session'] != $this->session
                ) {
                    throw new Mmq_JobException('Cannot release job not reserved by this session');
                }

                $updates = array(
                    'session'       => 0,
                    'ts_available'  => $now + max(0, (int) $delay),
                    'ts_updated'    => $now,
                );

                if ($pri >= 0) {
                    $updates['pri'] = $this->filterPriority($pri);
                }

                $this->mysql->update('job', $updates, array(
                    'id'    => $id,
                ));
            }

        } catch (Exception $ex) {
            $this->mysql->rollback();
            throw $ex;
        }

        return $result;
    }

    public function bury($id, $pri = -1)
    {
        $result = false;
        $this->mysql->begin();

        try {
            $job = $this->mysql->findForUpdate('job', array(
                'id'    => $id,
            ));

            if ($job && $job['state'] != self::STATE_DELETED) {
                $result = true;

                $now = time();

                if (
                    $job['state'] == self::STATE_READY
                    && $job['ts_available'] > $now
                    && $job['session']
                    && $job['session'] != $this->session
                ) {
                    throw new Mmq_JobException('Cannot bury job reserved by some other session');
                }

                $updates = array(
                    'pri'           => $pri,
                    'state'         => self::STATE_BURIED,
                    'ts_updated'    => $now,
                );

                if ($pri >= 0) {
                    $updates['pri'] = $this->filterPriority($pri);
                }

                $this->mysql->update('job', $updates, array(
                    'id'            => $id,
                ));
            }

        } catch (Exception $ex) {
            $this->mysql->rollback();
            throw $ex;
        }

        return $result;
    }

}
