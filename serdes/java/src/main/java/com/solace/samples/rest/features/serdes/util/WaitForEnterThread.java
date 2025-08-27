/*
 * Copyright 2025 Solace Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.solace.samples.rest.features.serdes.util;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class WaitForEnterThread extends Thread {
    private final AtomicBoolean done = new AtomicBoolean(false);

    public WaitForEnterThread() {
        System.out.println("Press Enter to exit...");
    }

    @Override
    public void run() {
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        done.set(true);
        scanner.close();
    }

    public boolean isDone() {
        return done.get();
    }
}
