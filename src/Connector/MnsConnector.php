<?php
/**
 * This file is part of the Pandawa Mns Queue package.
 *
 * (c) 2018 Pandawa <https://github.com/pandawa/pandawa>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Pandawa\Mns\Connector;

use Aliyun\MNS\Client;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Pandawa\Mns\Adapter\MnsAdapter;
use Pandawa\Mns\Queue\MnsQueue;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class MnsConnector implements ConnectorInterface
{
    /**
     * {@inheritdoc}
     */
    public function connect(array $config): Queue
    {
        return new MnsQueue($this->getAdapter($config), $config['queue'], $config['wait_seconds'] ?? null);
    }

    /**
     * @param array $config
     *
     * @return string
     */
    private function getEndpoint(array $config): string
    {
        return str_replace('(s)', 's', $config['endpoint']);
    }

    /**
     * @param array $config
     *
     * @return MnsAdapter
     */
    private function getAdapter(array $config): MnsAdapter
    {
        return new MnsAdapter(
            new Client(
                $this->getEndpoint($config),
                $config['key'],
                $config['secret']
            )
        );
    }
}