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

namespace Pandawa\Mns;

use Illuminate\Queue\QueueManager;
use Pandawa\Component\Module\AbstractModule;
use Pandawa\Mns\Connector\MnsConnector;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class PandawaMnsModule extends AbstractModule
{
    protected function init(): void
    {
        $this->registerConnector($this->app->get('queue'));
    }

    /**
     * @param QueueManager $queueManager
     */
    private function registerConnector(QueueManager $queueManager): void
    {
        $queueManager->addConnector(
            'mns',
            function () {
                return new MnsConnector();
            }
        );
    }
}