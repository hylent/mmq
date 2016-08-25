<?php

class Mmq
{
    const ERR_SESSION       = 0x1010;
    const ERR_TUBE          = 0x1020;
    const ERR_JOB           = 0x1030;

    const STATE_READY       = 0;
    const STATE_BURIED      = 1;
    const STATE_DELETED     = 2;

    protected $mysqli;

    protected $session;
    protected $sessionUsedTube;
    protected $sessionWatchedTubes;

    public function __construct($mysqli, $tube = 'default', $session = '', $sessionTries = 3)
    {
        // Set mysqli
        $this->mysqli = $mysqli;

        // Set session when provided
        $session = (string) $session;
        if (preg_match('/^\d+$/', $session)) {
            $sessionData = $this->dbFind('session', array(
                'session'   => $session,
            ));
            if (!$sessionData) {
                throw $this->newException(self::ERR_SESSION, sprintf(
                    'Session %s not found',
                    $session
                ));
            }
            $this->dbUpdate('session', array(
                'ts_updated'    => time(),
            ), array(
                'session'       => $session,
            ));
            $this->session              = $session;
            $this->sessionUsedTube      = $sessionData['tube'];
            $this->sessionWatchedTubes  = $this->dbFindIndexedList('session', 'tube', array(
                'session'   => $session,
            ));
            return;
        }

        // Set session when generated
        $tube = $this->filterTube($tube);
        for ($i = 1; $i <= $sessionTries; $i++) {
            $session = mt_rand(100000000, 999999999).mt_rand(100000000, 999999999);
            $sessionData = $this->dbFind('session', array(
                'session'   => $session,
            ));
            if ($sessionData) {
                continue;
            }
            $sessionData = array(
                'session'       => $session,
                'tube'          => $tube,
                'ts_created'    => time(),
                'ts_updated'    => time(),
            );
            $this->dbInsert('session', $sessionData);
            $this->dbInsert('session_tube', array(
                'session'       => $session,
                'tube'          => $tube,
            ));
            $this->session              = $session;
            $this->sessionUsedTube      = $tube;
            $this->sessionWatchedTubes  = array(
                $tube   => $tube,
            );
            return;
        }

        throw $this->newException(self::ERR_SESSION, sprintf(
            'Failed starting session in %d tries',
            $sessionTries
        ));
    }

    public function listTubes()
    {
        return $this->dbFindList('job', 'distinct session', array(
            'state<>'   => self::STATE_DELETED,
        ));
    }

    public function useTube($tube)
    {
        $tube = $this->filterTube($tube);

        if ($this->sessionUsedTube == $tube) {
            return;
        }

        $this->dbUpdate('session', array(
            'tube'          => $tube,
        ), array(
            'session'       => $session,
            'ts_updated'    => time(),
        ));
    }

    public function getUsedTube()
    {
        return $this->sessionUsedTube;
    }

    public function put($data, $ttr = 0, $pri = -1, $delay = 0)
    {
        $data   = $this->filterData($data);
        $ttr    = $this->filterTtr($ttr);
        $pri    = $this->filterPriority($pri);
        $delay  = $this->filterDelay($delay);

        return $this->dbInsert('job', array(
            'data'          => $data,
            'ttr'           => $ttr,
            'pri'           => $pri,
            'tube'          => $this->sessionUsedTube,
            'state'         => self::STATE_READY,
            'session'       => 0,
            'ts_available'  => 'current_timestamp() + '.$delay,
            'ts_created'    => 'current_timestamp()',
            'ts_updated'    => 'current_timestamp()',
        ));
    }

    public function kickJobs($bound)
    {
        $bound = max(1, (int) $bound);

        return $this->dbUpdate('job', array(
            'session'       => 0,
            'state'         => self::STATE_READY,
            'ts_updated'    => 'current_timestamp()',
        ), array(
            'state'         => self::STATE_BURIED,
            'tube'          => $this->sessionUsedTube,
        ), array(
            'pri'           => 'asc',
            'ts_available'  => 'asc',
            'id'            => 'asc',
        ), $bound);
    }

    public function kick($id)
    {
        return 1 == $this->dbUpdate('job', array(
            'session'       => 0,
            'state'         => self::STATE_READY,
            'ts_updated$'   => 'current_timestamp()',
        ), array(
            'id'            => $id,
            'state'         => self::STATE_BURIED,
        ));
    }

    public function watchTube($tube)
    {
        $tube = $this->filterTube($tube);

        if (!isset($this->sessionWatchedTubes[$tube])) {
            $this->dbInsert('session_tube', array(
                'session'   => $session,
                'tube'      => $tube,
            ));
            $this->sessionWatchedTubes[$tube] = $tube;
        }

        return count($this->sessionWatchedTubes);
    }

    public function ignoreTube($tube)
    {
        $tube = $this->filterTube($tube);

        $cnt = count($this->sessionWatchedTubes);

        if (isset($this->sessionWatchedTubes[$tube])) {
            if ($cnt == 1) {
                throw $this->newException(self::ERR_SESSION, sprintf(
                    'Last watched tube %s cannot be ignored',
                    $tube
                ));
            }

            $this->dbDelete('session_tube', array(
                'session'   => $session,
                'tube'      => $tube,
            ));
            unset($this->sessionWatchedTubes[$tube]);
            $cnt--;
        }

        return $cnt;
    }

    public function listWatchedTubes()
    {
        return $this->sessionWatchedTubes;
    }

    public function reserve($timeout = 0)
    {
        $cntTries = $this->dbUpdateReturningId(array(
            'cnt_tries$'    => 'last_insert_id(cnt_tries + 1)',
        ), array(
            'session'       => $this->session,
        ));

        $expire = time() + max(0, (int) $timeout);

        for ($i = 0; time() <= $expire; $i++) {
            $affectedRows = $this->dbUpdate('job', array(
                'session'           => $this->session,
                'ts_available$expr' => 'current_timestamp() + ttr',
                'ts_updated'        => 'current_timestamp()',
                'cnt_tries'         => $cntTries,
            ), array(
                'state'             => self::STATE_READY,
                'tube$in'           => $this->sessionWatchedTubes,
                'ts_available<'     => 'current_timestamp()',
            ), array(
                'pri'               => 'asc',
                'ts_available'      => 'asc',
                'id'                => 'asc',
            ), 1);

            if ($affectedRows == 1) {
                return $this->dbFind('job', array(
                    'session'   => $this->session,
                    'cnt_tries' => $cntTries,
                ));
            }

            sleep(pow(2, $i));
        }
    }

    public function delete($id)
    {
        // Cannot delete job reserved by some other session

        return 1 == $this->dbUpdate('job', array(
            'state'         => self::STATE_DELETED,
            'ts_updated'    => 'current_timestamp()',
        ), array(
            'id'    => $id,
            '$or'   => array(
                'state' => self::STATE_BURIED,
                '$and' => array(
                    'state'             => self::STATE_READY,
                    'ts_available>=$'   => 'current_timestamp()',
                    'session'           => $this->session,
                ),
            ),
        ));
    }

    public function touch($id)
    {
        // Cannot touch job not reserved by this session

        return 1 == $this->dbUpdate('job', array(
            'ts_available$expr' => 'current_timestamp() + ttr',
            'ts_updated'        => 'current_timestamp()',
        ), array(
            'id'                => $id,
            'state'             => self::STATE_READY,
            'ts_available>=$'   => 'current_timestamp()',
            'session'           => $this->session,
        ));
    }

    public function release($id, $pri = -1, $delay = 0)
    {
        // Cannot release job not reserved by this session

        $delay = $this->filterDelay($delay);

        $updates = array(
            'session'       => 0,
            'ts_available'  => 'current_timestamp() + '.$delay,
            'ts_updated'    => 'current_timestamp()',
        );

        if ($pri >= 0) {
            $updates['pri'] = $this->filterPriority($pri);
        }

        return 1 == $this->dbUpdate('job', $updates, array(
            'id'                => $id,
            'state'             => self::STATE_READY,
            'ts_available>=$'   => 'current_timestamp()',
            'session'           => $this->session,
        ));
    }

    public function bury($id, $pri = -1)
    {
        // Cannot bury job reserved by some other session

        $updates = array(
            'pri'           => $pri,
            'state'         => self::STATE_BURIED,
            'ts_updated$'   => 'current_timestamp()',
        );

        if ($pri >= 0) {
            $updates['pri'] = $this->filterPriority($pri);
        }

        return 1 == $this->dbUpdate('job', $updates, array(
            'id'    => $id,
            'state' => self::STATE_READY,
            '$or'   => array(
                'ts_available>=$'   => 'current_timestamp()',
                'session$in'        => array(0, $this->session),
            ),
        ));
    }

    protected function newException($code, $message)
    {
        return new Exception($message, $code);
    }

    protected function filterTube($tube)
    {
        $tube = (string) $tube;

        if ($tube === '') {
            throw $this->newException(self::ERR_JOB, 'Empty tube');
            throw new Mmq_TubeException('Empty tube');
        }
        if (strlen($tube) > 200) {
            throw $this->newException(self::ERR_JOB, sprintf(
                'Invalid tube %s',
                $tube
            ));
        }

        return $tube;
    }

    protected function filterData($data)
    {
        $data = (string) $data;
        $dataLength = mb_strlen($data, 'utf-8');
        if ($dataLength > 2000) {
            throw $this->newException(self::ERR_JOB, sprintf(
                'Job data length %d is too large',
                $dataLength
            ));
        }

        return $data;
    }

    protected function filterTtr($ttr)
    {
        $ttr = (int) $ttr;

        return $ttr < 1 ? 90 : $ttr;
    }

    protected function filterPriority($pri)
    {
        $pri = (int) $pri;

        return $pri < 0 ? 1024 : $pri;
    }

    protected function filterDelay($delay)
    {
        $delay = (int) $delay;

        return $delay < 0 ? 0 : $delay;
    }

    protected function dbFind()
    {
    }

    protected function dbFindList()
    {
    }

    protected function dbFindIndexedList()
    {
    }

    protected function dbDelete()
    {
    }

    protected function dbInsert()
    {
    }

    protected function dbUpdate()
    {
    }

    protected function dbUpdateReturningId()
    {
    }
}
